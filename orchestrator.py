#!/usr/bin/env python3
# Orchestrator: H.264 video, WAV audio, PIR motion events, and continuous PIR raw diagnostics.
# Outputs per session:
#   - video.h264     (H.264 elementary stream, inline SPS/PPS)
#   - video.pts      (per-frame PTS timestamps from libcamera-vid)
#   - audio.wav      (PCM S16_LE WAV)
#   - events.jsonl   (newline-delimited events with ts_mono_ns + ts_wall_ns)
#   - pir_raw.txt    (CSV: ts_wall_ns, ts_mono_ns, adc, oor, cfg_mirror[, raw_hex])
#
# Usage examples:
#   python3 orchestrator.py
#   python3 orchestrator.py --duration 600 --audio-device plughw:2,0 --video-width 1280 --video-height 720 --video-fps 30

import asyncio, json, os, time, signal, socket, platform, argparse, contextlib, re, sys
from pathlib import Path

# ========== Timebase (monotonic for ordering; wall from fixed offset) ==========
_offset_ns = time.time_ns() - time.monotonic_ns()
now_mono = time.monotonic_ns
def wall_from_mono(ts_mono_ns: int) -> int:
    return ts_mono_ns + _offset_ns

# ========== Session helpers ==========
def make_session_root(root="dataset") -> Path:
    session_id = time.strftime("%Y%m%d_%H%M%S_session", time.localtime())
    p = Path(root) / session_id
    p.mkdir(parents=True, exist_ok=True)
    return p

class JsonlWriter:
    def __init__(self, path: Path):
        self._f = open(path, "a", buffering=1)
    def write(self, obj: dict):
        self._f.write(json.dumps(obj, separators=(",", ":")) + "\n")
    def close(self):
        with contextlib.suppress(Exception):
            self._f.close()

# ========== Audio (WAV via arecord) ==========
async def run_audio_wav(queue: asyncio.Queue, out_wav: Path, *, device="plughw:2,0", rate=48000, chunk_ms=100):
    """
    Capture PCM S16_LE WAV from ALSA. Emit an event roughly each chunk_ms.
    """
    cmd = [
        "arecord",
        "-D", device,
        "-c", "1",
        "-r", str(rate),
        "-f", "S16_LE",
        "-t", "wav",
        "-"  # stdout
    ]
    BYTES_PER_SAMPLE = 2
    CHUNK_BYTES = max(1, int(rate * (chunk_ms/1000.0) * BYTES_PER_SAMPLE))

    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    except Exception as e:
        ts = now_mono()
        await queue.put({
            "ts_mono_ns": ts,
            "ts_wall_ns": wall_from_mono(ts),
            "source": "system",
            "level": "error",
            "meta": {"component": "audio"},
            "payload": {"event": "audio_spawn_failed", "error": str(e), "device": device}
        })
        return

    seq = 0
    bytes_written = 0
    buf = b""
    with open(out_wav, "ab", buffering=0) as wav_out:
        try:
            while True:
                data = await proc.stdout.read(8192)
                if not data:
                    # capture any stderr into a system event
                    err = (await proc.stderr.read()).decode("utf-8", "ignore")
                    if err:
                        ts = now_mono()
                        await queue.put({
                            "ts_mono_ns": ts,
                            "ts_wall_ns": wall_from_mono(ts),
                            "source": "system",
                            "level": "error",
                            "meta": {"component": "audio"},
                            "payload": {"event": "audio_exited", "stderr": err}
                        })
                    break
                buf += data
                if buf:
                    wav_out.write(buf)
                    bytes_written += len(buf)
                    while len(buf) >= CHUNK_BYTES:
                        ts = now_mono()
                        await queue.put({
                            "ts_mono_ns": ts,
                            "ts_wall_ns": wall_from_mono(ts),
                            "source": "audio",
                            "seq": seq,
                            "meta": {"fmt":"wav","pcm_fmt":"S16_LE","rate":rate,"device":device,"chunk_ms":chunk_ms},
                            "payload": {"chunk_bytes": CHUNK_BYTES, "file_bytes_total": bytes_written}
                        })
                        buf = buf[CHUNK_BYTES:]
                        seq += 1
        except asyncio.CancelledError:
            pass
        finally:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            with contextlib.suppress(Exception):
                await proc.wait()

# ========== Video (H.264 via libcamera-vid + PTS tail) ==========
async def run_camera_h264_with_pts(queue: asyncio.Queue, out_h264: Path, pts_path: Path, *, w=1280, h=720, fps=30):
    """
    Start libcamera-vid writing H.264 to file and PTS lines to a sidecar file. Tail PTS for per-frame events.
    """
    cmd = [
        "libcamera-vid",
        "-t","0",
        "--codec","h264",
        "--inline",
        "--profile","baseline",
        "--level","4.1",
        "--width", str(w),
        "--height", str(h),
        "--framerate", str(fps),
        "-o", str(out_h264),
        "--save-pts", str(pts_path)
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    except Exception as e:
        ts = now_mono()
        await queue.put({
            "ts_mono_ns": ts,
            "ts_wall_ns": wall_from_mono(ts),
            "source": "system",
            "level": "error",
            "meta": {"component": "camera"},
            "payload": {"event": "camera_spawn_failed", "error": str(e)}
        })
        return

    # Wait for PTS file to appear
    for _ in range(200):
        if pts_path.exists():
            break
        await asyncio.sleep(0.05)

    tail = None
    if pts_path.exists():
        tail = await asyncio.create_subprocess_exec(
            "tail","-F",str(pts_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            text=True
        )

    seq = 0
    try:
        if tail is None:
            # If PTS never appeared, still keep camera alive, but log a warning
            ts = now_mono()
            await queue.put({
                "ts_mono_ns": ts,
                "ts_wall_ns": wall_from_mono(ts),
                "source": "system",
                "level": "warning",
                "meta": {"component": "camera"},
                "payload": {"event": "pts_missing"}
            })
            # idle until cancel
            while True:
                await asyncio.sleep(1.0)
        else:
            while True:
                line = await tail.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not re.fullmatch(r"\d+", line):
                    continue
                pts_us = int(line)
                ts = now_mono()
                await queue.put({
                    "ts_mono_ns": ts,
                    "ts_wall_ns": wall_from_mono(ts),
                    "source": "camera",
                    "seq": seq,
                    "meta": {"fmt":"h264","w":w,"h":h,"fps":fps,"pts_units":"us"},
                    "payload": {"pts_us": pts_us}
                })
                seq += 1
    except asyncio.CancelledError:
        pass
    finally:
        if tail:
            with contextlib.suppress(ProcessLookupError):
                tail.terminate()
            with contextlib.suppress(Exception):
                await tail.wait()
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        # bubble up any stderr lines for debugging
        with contextlib.suppress(Exception):
            err = (await proc.stderr.read()).decode("utf-8","ignore")
            if err:
                ts = now_mono()
                await queue.put({
                    "ts_mono_ns": ts,
                    "ts_wall_ns": wall_from_mono(ts),
                    "source": "system",
                    "level": "info",
                    "meta": {"component": "camera"},
                    "payload": {"event": "camera_stderr", "stderr": err}
                })
        with contextlib.suppress(Exception):
            await proc.wait()

# ========== PIR (your driver) ==========
async def run_pir_motion(queue: asyncio.Queue, *, gpiochip="/dev/gpiochip0", dl=23, serin=17,
                         cfg=None, include_raw_adc=True, heartbeat_sec=2.0):
    """
    Motion events using your pir_pyd1588.PYD1588 driver.
    Emits 'pir_ready' on start, motion events on triggers, and idle heartbeats.
    """
    if "/mnt/data" not in sys.path:
        sys.path.insert(0, "/mnt/data")

    ts0 = now_mono()
    await queue.put({
        "ts_mono_ns": ts0,
        "ts_wall_ns": wall_from_mono(ts0),
        "source": "system",
        "level": "info",
        "meta": {"component": "pir"},
        "payload": {"event": "pir_ready", "gpiochip": gpiochip, "dl": dl, "serin": serin}
    })

    try:
        from pir_pyd1588 import PYD1588, make_config
    except Exception as e:
        ts = now_mono()
        await queue.put({
            "ts_mono_ns": ts,
            "ts_wall_ns": wall_from_mono(ts),
            "source": "system",
            "level": "error",
            "meta": {"component": "pir"},
            "payload": {"event": "pir_driver_import_failed", "error": str(e)}
        })
        return

    sensor = PYD1588()
    sensor.gpiochip = gpiochip
    sensor.dl = dl
    sensor.serin = serin
    cfg = cfg or dict(threshold=20, blind=3, pulses=1, wtime=1, opmode=2, countmode=0, source=0)

    try:
        sensor.write_config(make_config(**cfg))
        sensor.arm_wakeup()
    except Exception as e:
        ts = now_mono()
        await queue.put({
            "ts_mono_ns": ts,
            "ts_wall_ns": wall_from_mono(ts),
            "source": "system",
            "level": "error",
            "meta": {"component": "pir"},
            "payload": {"event": "pir_init_failed", "error": str(e)}
        })
        return

    loop = asyncio.get_running_loop()
    seq = 0
    last_hb = time.monotonic()

    def _wait_blocking():
        # Your driver: True on motion, False on timeout
        return sensor.wait_for_motion(1.0)

    try:
        while True:
            fired = await loop.run_in_executor(None, _wait_blocking)
            if fired:
                ts = now_mono()
                payload = {}
                if include_raw_adc:
                    with contextlib.suppress(Exception):
                        oor, adc, cfg_mirror = sensor.forced_read_packet()
                        payload.update({"adc": int(adc), "oor": bool(oor), "cfg_mirror": int(cfg_mirror)})

                await queue.put({
                    "ts_mono_ns": ts,
                    "ts_wall_ns": wall_from_mono(ts),
                    "source": "pir",
                    "seq": seq,
                    "meta": {
                        "driver": "PYD1588", "edge": "rising",
                        "chip": gpiochip, "line": dl, "serin": serin, "config": cfg
                    },
                    "payload": payload
                })
                seq += 1

                with contextlib.suppress(Exception):
                    sensor.clear_interrupt()
            else:
                # heartbeat while idle
                if time.monotonic() - last_hb >= heartbeat_sec:
                    ts = now_mono()
                    await queue.put({
                        "ts_mono_ns": ts,
                        "ts_wall_ns": wall_from_mono(ts),
                        "source": "pir",
                        "seq": seq,
                        "meta": {"driver":"PYD1588","edge":"heartbeat","chip":gpiochip,"line":dl,"serin":serin},
                        "payload": {}
                    })
                    last_hb = time.monotonic()
    except asyncio.CancelledError:
        pass
    finally:
        with contextlib.suppress(Exception):
            sensor.close()

# ========== PIR diagnostics (continuous amplitudes + raw) ==========
async def run_pir_diag_txt(txt_path: Path, *, gpiochip="/dev/gpiochip0", dl=23, serin=17, sample_hz=50.0):
    """
    Writes continuous samples to pir_raw.txt:
      ts_wall_ns, ts_mono_ns, adc, oor, cfg_mirror[, raw_hex]
    Uses your driver’s forced_read_packet(); if forced_read_raw_hex() exists, logs it too.
    """
    if "/mnt/data" not in sys.path:
        sys.path.insert(0, "/mnt/data")

    try:
        from pir_pyd1588 import PYD1588, make_config
    except Exception as e:
        with open(txt_path, "a", buffering=1) as f:
            f.write(f"# PIR diag disabled: import error: {e}\n")
        return

    sensor = PYD1588()
    sensor.gpiochip = gpiochip
    sensor.dl = dl
    sensor.serin = serin
    cfg = dict(threshold=20, blind=3, pulses=1, wtime=1, opmode=2, countmode=0, source=0)

    try:
        sensor.write_config(make_config(**cfg))
    except Exception as e:
        with open(txt_path, "a", buffering=1) as f:
            f.write(f"# PIR diag init failed: {e}\n")
        return

    header_written = False
    period = 1.0 / float(sample_hz)
    try:
        with open(txt_path, "a", buffering=1) as f:
            while True:
                try:
                    oor, adc, cfg_mirror = sensor.forced_read_packet()
                    raw_hex = None
                    # Optional: if your driver exposes a raw-hex method, include it
                    try:
                        raw_hex = sensor.forced_read_raw_hex()  # safe to fail if not implemented
                    except Exception:
                        raw_hex = None

                    ts_mono = now_mono()
                    ts_wall = wall_from_mono(ts_mono)

                    if not header_written and (txt_path.stat().st_size == 0):
                        # Include raw_hex column only if available at least once
                        if raw_hex is None:
                            f.write("# ts_wall_ns, ts_mono_ns, adc, oor, cfg_mirror\n")
                        else:
                            f.write("# ts_wall_ns, ts_mono_ns, adc, oor, cfg_mirror, raw_hex\n")
                        header_written = True

                    if raw_hex is None:
                        f.write(f"{ts_wall},{ts_mono},{int(adc)},{1 if oor else 0},{int(cfg_mirror)}\n")
                    else:
                        f.write(f"{ts_wall},{ts_mono},{int(adc)},{1 if oor else 0},{int(cfg_mirror)},{raw_hex}\n")

                except Exception as e:
                    f.write(f"# read_error: {e}\n")

                await asyncio.sleep(period)
    except asyncio.CancelledError:
        pass
    finally:
        with contextlib.suppress(Exception):
            sensor.close()

# ========== CSI placeholder ==========
async def run_csi_placeholder(queue: asyncio.Queue):
    seq = 0
    try:
        while True:
            await asyncio.sleep(1.0)
            ts = now_mono()
            await queue.put({
                "ts_mono_ns": ts,
                "ts_wall_ns": wall_from_mono(ts),
                "source": "csi",
                "seq": seq,
                "meta": {"placeholder": True},
                "payload": {"note": "CSI capture not enabled yet"}
            })
            seq += 1
    except asyncio.CancelledError:
        pass

# ========== Single sink ==========
async def event_writer(queue: asyncio.Queue, jsonl_path: Path):
    writer = JsonlWriter(jsonl_path)
    try:
        while True:
            rec = await queue.get()
            writer.write(rec)
    except asyncio.CancelledError:
        pass
    finally:
        writer.close()

# ========== Main ==========
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="dataset")
    parser.add_argument("--duration", type=float, default=None, help="Seconds to record (default: until Ctrl+C)")
    parser.add_argument("--video-width", type=int, default=1280)
    parser.add_argument("--video-height", type=int, default=720)
    parser.add_argument("--video-fps", type=int, default=30)
    parser.add_argument("--audio-device", default="plughw:2,0")
    parser.add_argument("--audio-rate", type=int, default=48000)
    parser.add_argument("--audio-chunk-ms", type=int, default=100)
    parser.add_argument("--pir-chip", default="/dev/gpiochip0")
    parser.add_argument("--pir-dl", type=int, default=23)
    parser.add_argument("--pir-serin", type=int, default=17)
    parser.add_argument("--pir-sample-hz", type=float, default=50.0, help="Diagnostics sampling rate for pir_raw.txt")
    args = parser.parse_args()

    session_root = make_session_root(args.root)
    events_jsonl = session_root / "events.jsonl"
    video_h264 = session_root / "video.h264"
    video_pts  = session_root / "video.pts"
    audio_wav  = session_root / "audio.wav"
    pir_diag   = session_root / "pir_raw.txt"

    # Session meta
    meta = {
        "session_id": session_root.name,
        "created_wall_ns": time.time_ns(),
        "created_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "tz": (time.tzname[0] if time.tzname else "unknown"),
        "clock_offset_ns": _offset_ns,
        "video": {"fmt":"h264","path": video_h264.name, "pts_file": video_pts.name,
                  "w": args.video_width, "h": args.video_height, "fps": args.video_fps, "inline": True},
        "audio": {"fmt":"wav","path": audio_wav.name, "rate": args.audio_rate, "pcm_fmt":"S16_LE","device": args.audio_device},
        "pir": {"chip": args.pir_chip, "dl": args.pir_dl, "serin": args.pir_serin, "diag_hz": args.pir_sample_hz},
        "csi": {"enabled": False}
    }
    (session_root / "session_meta.json").write_text(json.dumps(meta, indent=2))
    (session_root / "README.txt").write_text(
        "This folder is a single recording session.\n"
        "- video.h264 : H.264 elementary stream (inline SPS/PPS). Use ffmpeg to remux to MP4/MKV without re-encode.\n"
        "- video.pts  : per-frame timestamps (usually microseconds) emitted by libcamera-vid --save-pts.\n"
        "- audio.wav  : 1-channel PCM S16_LE at configured rate.\n"
        "- events.jsonl : newline-delimited events with ts_mono_ns + ts_wall_ns for all modalities.\n"
        "- pir_raw.txt : CSV with continuous PIR amplitudes and raw fields (oor, cfg_mirror[, raw_hex]) at fixed rate.\n"
    )
    (session_root / ".in_progress").write_text("1")

    q = asyncio.Queue(maxsize=2000)
    tasks = [
        asyncio.create_task(event_writer(q, events_jsonl)),
        asyncio.create_task(run_camera_h264_with_pts(
            q, video_h264, video_pts,
            w=args.video_width, h=args.video_height, fps=args.video_fps
        )),
        asyncio.create_task(run_audio_wav(
            q, audio_wav, device=args.audio_device, rate=args.audio_rate, chunk_ms=args.audio_chunk_ms
        )),
        asyncio.create_task(run_pir_motion(
            q, gpiochip=args.pir_chip, dl=args.pir_dl, serin=args.pir_serin,
            include_raw_adc=True, heartbeat_sec=2.0
        )),
        asyncio.create_task(run_pir_diag_txt(
            pir_diag, gpiochip=args.pir_chip, dl=args.pir_dl, serin=args.pir_serin,
            sample_hz=args.pir_sample_hz
        )),
        asyncio.create_task(run_csi_placeholder(q)),
    ]

    # Graceful shutdown
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    def _cancel_all():
        for t in tasks: t.cancel()
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _cancel_all)

    try:
        if args.duration is not None:
            try:
                await asyncio.wait_for(asyncio.Event().wait(), timeout=args.duration)
            except asyncio.TimeoutError:
                pass
            finally:
                _cancel_all()
        await stop.wait()
    finally:
        await asyncio.gather(*tasks, return_exceptions=True)
        (session_root / ".in_progress").unlink(missing_ok=True)
        (session_root / ".complete").write_text("1")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

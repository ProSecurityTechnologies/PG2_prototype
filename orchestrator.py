#!/usr/bin/env python3
# Orchestrator that saves directly in standard formats:
#   - video.h264 (H.264 from libcamera-vid, inline SPS/PPS)
#   - audio.wav  (WAV/PCM from arecord)
#   - events.jsonl (timestamps for camera frames, audio chunks, PIR edges, CSI heartbeat)

import asyncio, json, os, time, signal, socket, platform, argparse, contextlib, re
from pathlib import Path
import gpiod  # sudo apt install python3-libgpiod

# ========= Timebase =========
offset_ns = time.time_ns() - time.monotonic_ns()
now_mono = time.monotonic_ns
def wall_from_mono(ts_mono_ns: int) -> int:
    return ts_mono_ns + offset_ns

# ========= Session helpers =========
def make_session_root(root="dataset") -> Path:
    session_id = time.strftime("%Y%m%d_%H%M%S_session", time.localtime())
    p = Path(root) / session_id
    p.mkdir(parents=True, exist_ok=True)
    return p

class JsonlWriter:
    def __init__(self, path: Path):
        self.f = open(path, "a", buffering=1)
    def write(self, obj: dict):
        self.f.write(json.dumps(obj, separators=(",", ":")) + "\n")
    def close(self):
        with contextlib.suppress(Exception):
            self.f.close()

# ========= Producers =========

async def run_audio_wav(queue: asyncio.Queue, out_wav: Path, *, device="plughw:2,0", rate=48000, chunk_ms=100):
    """
    Capture WAV/PCM directly from arecord on stdout; we write it to audio.wav and emit an event per ~chunk_ms.
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
    CHUNK_BYTES = int(rate * (chunk_ms/1000.0) * BYTES_PER_SAMPLE)

    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
    seq = 0
    bytes_written = 0
    buf = b""
    with open(out_wav, "ab", buffering=0) as wav_out:
        try:
            while True:
                # Read in reasonably sized chunks
                data = await proc.stdout.read(max(CHUNK_BYTES - len(buf), 4096))
                if not data:
                    break
                buf += data
                # Flush all buffered data to file (we don't need to align writes to CHUNK strictly)
                wav_out.write(buf)
                bytes_written += len(buf)
                # Emit events in ~chunk_ms cadence
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
                # keep any residual bytes in buf so cadence remains stable
        finally:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            with contextlib.suppress(Exception):
                await proc.wait()

async def run_camera_h264_with_pts(queue: asyncio.Queue, out_h264: Path, pts_path: Path, *, w=1280, h=720, fps=30):
    """
    Record H.264 directly to file using libcamera-vid and save per-frame PTS to a sidecar file.
    We tail the PTS file and emit one event per frame with original PTS.
    """
    # libcamera-vid writes file itself, so we start it and separately tail the PTS file for frame timing.
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
    proc = await asyncio.create_subprocess_exec(*cmd)
    # Tail the PTS file for frame events. Format is one timestamp per line (usually microseconds).
    # We'll use `tail -F` for robustness across file rotations (libcamera-vid appends).
    # Wait until PTS file exists:
    while not pts_path.exists():
        await asyncio.sleep(0.05)
    tail = await asyncio.create_subprocess_exec("tail","-F",str(pts_path),
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.DEVNULL,
                                                text=True)
    seq = 0
    try:
        while True:
            line = await tail.stdout.readline()
            if not line:
                break
            line = line.strip()
            # Typically PTS is in microseconds; accept integers only
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
        with contextlib.suppress(ProcessLookupError):
            tail.terminate()
        with contextlib.suppress(Exception):
            await tail.wait()
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        with contextlib.suppress(Exception):
            await proc.wait()

async def run_pir_pyd1588(queue: asyncio.Queue, *, gpiochip="/dev/gpiochip0", dl=23, serin=17,
                          cfg=None, include_raw_adc=False):
    """
    Adapter for your PYD1588 driver. If you don't want this, you can swap back to a gpiod edge reader.
    """
    try:
        from pir_pyd1588 import PYD1588, make_config
    except Exception as e:
        # Fallback: simple gpiod edge reader if your module isn't available
        chip = gpiod.Chip(gpiochip); line = chip.get_line(dl)
        line.request(consumer="pir", type=gpiod.LINE_REQ_EV_BOTH_EDGES)
        seq = 0
        try:
            while True:
                if not line.event_wait(sec=10):
                    continue
                e = line.event_read()
                edge = "rising" if e.type == gpiod.LineEvent.RISING_EDGE else "falling"
                ts = now_mono()
                await queue.put({
                    "ts_mono_ns": ts,
                    "ts_wall_ns": wall_from_mono(ts),
                    "source": "pir",
                    "seq": seq,
                    "meta": {"edge": edge, "line": dl, "chip": gpiochip, "driver":"gpiod"},
                    "payload": {}
                })
                seq += 1
        finally:
            with contextlib.suppress(Exception): line.release()
            with contextlib.suppress(Exception): chip.close()
        return

    # Use your driver
    sensor = PYD1588()
    sensor.gpiochip = gpiochip
    sensor.dl = dl
    sensor.serin = serin
    cfg = cfg or dict(threshold=20, blind=3, pulses=1, wtime=1, opmode=2, countmode=0, source=0)
    sensor.write_config(make_config(**cfg))
    sensor.arm_wakeup()

    seq = 0
    loop = asyncio.get_running_loop()
    try:
        while True:
            fired = await loop.run_in_executor(None, sensor.wait_for_motion, 1.0)
            if not fired:
                continue
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
                "meta": {"edge": "rising", "chip": gpiochip, "line": dl, "serin": serin, "driver":"PYD1588", "config": cfg},
                "payload": payload
            })
            seq += 1
            sensor.clear_interrupt()
    except asyncio.CancelledError:
        pass
    finally:
        with contextlib.suppress(Exception): sensor.close()

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

# ========= Single log sink =========
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

# ========= Main =========
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="dataset", help="Dataset root folder")
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
    parser.add_argument("--pir-raw-adc", action="store_true")
    args = parser.parse_args()

    session_root = make_session_root(args.root)
    events_jsonl = session_root / "events.jsonl"
    video_h264 = session_root / "video.h264"
    video_pts  = session_root / "video.pts"
    audio_wav  = session_root / "audio.wav"

    # Session meta
    meta = {
        "session_id": session_root.name,
        "created_wall_ns": time.time_ns(),
        "created_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "tz": (time.tzname[0] if time.tzname else "unknown"),
        "clock_offset_ns": offset_ns,
        "video": {"fmt":"h264","path": video_h264.name, "pts_file": video_pts.name,
                  "w": args.video_width, "h": args.video_height, "fps": args.video_fps, "inline": True},
        "audio": {"fmt":"wav","path": audio_wav.name, "rate": args.audio_rate, "pcm_fmt":"S16_LE","device": args.audio_device},
        "pir": {"chip": args.pir_chip, "dl": args.pir_dl, "serin": args.pir_serin},
        "csi": {"enabled": False}
    }
    (session_root / "session_meta.json").write_text(json.dumps(meta, indent=2))
    (session_root / "README.txt").write_text(
        "This folder is a single recording session.\n"
        "- video.h264 : H.264 elementary stream (inline SPS/PPS). Use ffmpeg to remux to MP4/MKV without re-encode.\n"
        "- video.pts  : per-frame timestamps (usually microseconds) emitted by libcamera-vid --save-pts.\n"
        "- audio.wav  : 1-channel PCM S16_LE at configured rate.\n"
        "- events.jsonl : newline-delimited events with ts_mono_ns + ts_wall_ns for all modalities.\n"
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
        asyncio.create_task(run_pir_pyd1588(
            q, gpiochip=args.pir_chip, dl=args.pir_dl, serin=args.pir_serin, include_raw_adc=args.pir_raw_adc
        )),
        asyncio.create_task(run_csi_placeholder(q)),
    ]

    # graceful shutdown
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

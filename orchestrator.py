#!/usr/bin/env python3
import asyncio, json, os, sys, time, signal, platform, socket
from pathlib import Path
from dataclasses import dataclass, asdict
import gpiod  # sudo apt install python3-libgpiod
from pir_pyd1588 import PYD1588, make_config

# ========= Session + timebase =========

offset_ns = time.time_ns() - time.monotonic_ns()
now_mono = time.monotonic_ns
def wall_from_mono(ts_mono_ns: int) -> int:
    return ts_mono_ns + offset_ns

@dataclass
class SessionMeta:
    session_id: str
    created_wall_ns: int
    created_iso: str
    host: str
    platform: str
    kernel: str
    tz: str
    clock_offset_ns: int
    video: dict
    audio: dict
    pir: dict
    csi: dict

def make_session_root(root="dataset") -> Path:
    session_id = time.strftime("%Y%m%d_%H%M%S_session", time.localtime())
    p = Path(root) / session_id
    p.mkdir(parents=True, exist_ok=True)
    return p

# ========= JSONL writer =========

class JsonlWriter:
    def __init__(self, path: Path):
        self.path = path
        self.f = open(self.path, "a", buffering=1)

    def write(self, obj: dict):
        self.f.write(json.dumps(obj, separators=(",", ":")) + "\n")

    def close(self):
        try: self.f.close()
        except: pass

# ========= Producers =========

async def run_audio(queue: asyncio.Queue, raw_path: Path, fmt: dict):
    """
    Read S32_LE mono 48k chunks from arecord and append to file.
    Log an event per chunk with byte offsets and sequence.
    """
    cmd = [
        "arecord",
        "-D", fmt.get("device", "hw:2,0"),
        "-c", "1",
        "-r", str(fmt.get("rate", 16000)),
        "-f", "S16_LE",
        "-q",
        "-t", "raw",
        "-"
    ]
    CHUNK_S = fmt.get("chunk_ms", 100) / 1000.0
    BYTES_PER_SAMPLE = 4
    CHUNK_BYTES = int(fmt.get("rate", 48000) * CHUNK_S * BYTES_PER_SAMPLE)

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE
    )
    seq = 0
    buf = b""
    bytes_written = 0
    with open(raw_path, "ab", buffering=0) as audio_out:
        try:
            while True:
                need = CHUNK_BYTES - len(buf)
                data = await proc.stdout.read(need if need > 0 else CHUNK_BYTES)
                if not data:
                    break
                buf += data
                if len(buf) >= CHUNK_BYTES:
                    ts = now_mono()
                    audio_out.write(buf[:CHUNK_BYTES])
                    bytes_written += CHUNK_BYTES
                    await queue.put({
                        "ts_mono_ns": ts,
                        "ts_wall_ns": wall_from_mono(ts),
                        "source": "audio",
                        "seq": seq,
                        "meta": {
                            "rate": fmt.get("rate", 48000),
                            "fmt": "S32_LE",
                            "chunk_ms": fmt.get("chunk_ms", 100),
                            "bytes_per_sample": BYTES_PER_SAMPLE,
                            "device": fmt.get("device", "hw:1,0")
                        },
                        "payload": {
                            "chunk_bytes": CHUNK_BYTES,
                            "file_bytes_total": bytes_written
                        }
                    })
                    buf = buf[CHUNK_BYTES:]
                    seq += 1
        finally:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            await proc.wait()

async def run_camera(queue: asyncio.Queue, raw_path: Path, cfg: dict):
    """
    Stream YUV420 frames from libcamera-vid to stdout → file.
    Log frame events with seq and byte offsets.
    """
    w, h = cfg.get("width", 1280), cfg.get("height", 720)
    frame_bytes = int(w*h*1.5)
    cmd = [
        "libcamera-vid",
        "-t","0",
        "--codec","yuv420",
        "--nopreview",
        "-o","-",               # stdout
        "--width", str(w),
        "--height", str(h)
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE
    )
    seq, file_bytes = 0, 0
    buf = b""
    with open(raw_path, "ab", buffering=0) as vid_out:
        try:
            while True:
                need = frame_bytes - len(buf)
                chunk = await proc.stdout.read(need if need > 0 else frame_bytes)
                if not chunk:
                    break
                buf += chunk
                if len(buf) >= frame_bytes:
                    ts = now_mono()
                    frame = buf[:frame_bytes]
                    vid_out.write(frame)
                    file_bytes += frame_bytes
                    await queue.put({
                        "ts_mono_ns": ts,
                        "ts_wall_ns": wall_from_mono(ts),
                        "source": "camera",
                        "seq": seq,
                        "meta": {"w": w, "h": h, "fmt": "yuv420", "frame_bytes": frame_bytes},
                        "payload": {"file_bytes_total": file_bytes}
                    })
                    buf = buf[frame_bytes:]
                    seq += 1
        finally:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            await proc.wait()

async def run_pir_pyd1588(queue, *,
                          gpiochip: str = "/dev/gpiochip0",
                          dl: int = 17,
                          serin: int = 27,
                          cfg: dict | None = None,
                          include_raw_adc: bool = False):
    """
    Uses your PYD1588 driver exactly as implemented:
      - set lines, write config via SERIN
      - arm wake-up on DL
      - wait_for_motion() with short timeouts so we can be cancellable
      - clear_interrupt() after each fire
      - optionally do a best-effort forced 40-bit raw read to get ADC amplitude
    """
    sensor = PYD1588()
    sensor.gpiochip = gpiochip
    sensor.dl = dl
    sensor.serin = serin

    # Default config close to your prior examples; adjust as you like
    # threshold, blind, pulses, wtime, opmode=2 (Wake-Up), countmode, source=0(BPF)
    cfg = cfg or dict(threshold=20, blind=3, pulses=1, wtime=1, opmode=2, countmode=0, source=0)

    # Push SERIN config and arm wake-up on DL
    sensor.write_config(make_config(**cfg))
    sensor.arm_wakeup()

    seq = 0
    loop = asyncio.get_running_loop()
    try:
        while True:
            # Run blocking call in a thread so asyncio remains responsive
            fired = await loop.run_in_executor(None, sensor.wait_for_motion, 1.0)
            if not fired:
                continue

            ts = now_mono()
            payload = {}
            if include_raw_adc:
                try:
                    oor, adc, cfg_mirror = sensor.forced_read_packet()
                    payload.update({"adc": int(adc), "oor": bool(oor), "cfg_mirror": int(cfg_mirror)})
                except Exception as e:
                    # Raw read is best-effort; don't break the pipeline
                    payload.update({"adc_read_error": str(e)})

            await queue.put({
                "ts_mono_ns": ts,
                "ts_wall_ns": wall_from_mono(ts),
                "source": "pir",
                "seq": seq,
                "meta": {
                    "edge": "rising",             # DL rising woke us up
                    "chip": gpiochip,
                    "line": dl,
                    "serin": serin,
                    "driver": "PYD1588",
                    "config": cfg
                },
                "payload": payload
            })
            seq += 1

            # Acknowledge event on the sensor and re-arm
            sensor.clear_interrupt()
    except asyncio.CancelledError:
        pass
    finally:
        sensor.close()


async def run_csi_placeholder(queue: asyncio.Queue, cfg: dict):
    """
    Placeholder for FeitCSI: emit a heartbeat so the pipeline is wired.
    Replace with a subprocess reader similar to audio/camera when you’re ready.
    """
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

# ========= Log sink (single writer) =========

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

# ========= Main orchestration =========

import contextlib

async def main():
    session_root = make_session_root("dataset")
    # Configs (tweak as needed)
    audio_cfg = {"device": "hw:1,0", "rate": 48000, "chunk_ms": 100}
    video_cfg = {"width": 1280, "height": 720}
    pir_cfg = {"chip": "/dev/gpiochip4", "line": 23}
    csi_cfg = {}

    # Files
    events_jsonl = session_root / "events.jsonl"
    video_raw = session_root / "video.yuv"
    audio_raw = session_root / "audio.raw"
    audio_sidecar = session_root / "audio.json"
    meta_path = session_root / "session_meta.json"

    # Session metadata
    meta = SessionMeta(
        session_id=session_root.name,
        created_wall_ns=time.time_ns(),
        created_iso=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        host=socket.gethostname(),
        platform=platform.platform(),
        kernel=platform.release(),
        tz=time.tzname[0] if time.tzname else "unknown",
        clock_offset_ns=offset_ns,
        video={"fmt": "yuv420", **video_cfg},
        audio={"fmt": "S32_LE", **audio_cfg},
        pir=pir_cfg,
        csi={"enabled": False, **csi_cfg},
    )
    (session_root / ".in_progress").write_text("1")
    (session_root / "README.txt").write_text(
        "This folder is a single recording session.\n"
        "events.jsonl: newline-delimited JSON event log with ts_mono_ns + ts_wall_ns\n"
        "video.yuv: raw YUV420 frames, size per frame = width*height*1.5 bytes\n"
        "audio.raw: S32_LE mono 48kHz\n"
    )
    (session_root / "session_meta.json").write_text(json.dumps(asdict(meta), indent=2))
    audio_sidecar.write_text(json.dumps({"path": "audio.raw", **audio_cfg, "fmt": "S32_LE"}, indent=2))

    q = asyncio.Queue(maxsize=2000)

    tasks = [
        asyncio.create_task(event_writer(q, events_jsonl)),
        asyncio.create_task(run_camera(q, video_raw, video_cfg)),
        asyncio.create_task(run_audio(q, audio_raw, audio_cfg)),
        asyncio.create_task(run_pir_pyd1588(
            q,
            gpiochip="/dev/gpiochip0",
            dl=23,          # <- your DL line
            serin=17,       # <- your SERIN (MOSI) line
            cfg=dict(threshold=20, blind=3, pulses=1, wtime=1, opmode=2, countmode=0, source=0),
            include_raw_adc=True   # set False if you don’t want the extra read
        )),
        asyncio.create_task(run_csi_placeholder(q, csi_cfg)),
    ]

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _cancel_all():
        for t in tasks:
            t.cancel()
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _cancel_all)

    try:
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

# main.py
import time
import argparse
from pir_pyd1588 import PYD1588, make_config

def run_raw(sensor: PYD1588, rate_hz: float = 40.0):
    # Forced Readout mode for raw amplitude
    cfg = make_config(
        threshold=10, blind=0, pulses=0, wtime=2,
        opmode=0,   # forced readout
        countmode=0, source=0  # BPF => signed ADC
    )
    sensor.write_config(cfg)
    period = 1.0 / rate_hz
    print("Streaming raw packets (Forced Readout). Ctrl+C to stop.")
    try:
        while True:
            oor, adc, cfg_echo = sensor.forced_read_packet()
            # Unformatted line for easy piping:
            # <unix_ts> OOR=<0|1> ADC=<signed14> CFG=0x<7hex>
            print(f"{time.time():.6f} OOR={oor} ADC={adc} CFG=0x{cfg_echo:07X}")
            # adjust rate (10–50 Hz typical)
            time.sleep(period)
    except KeyboardInterrupt:
        pass

def run_wakeup(sensor: PYD1588):
    # Sensitive motion detection for responsiveness
    cfg = make_config(threshold=5, blind=0, pulses=0, wtime=3, opmode=2, countmode=0, source=0)
    sensor.write_config(cfg)
    sensor.arm_wakeup()
    sensor.clear_interrupt()
    print("Wake-Up mode armed. Move in front of the sensor… Ctrl+C to stop.")
    t0 = time.monotonic()
    try:
        while True:
            fired = sensor.wait_for_motion(timeout_s=0.5)
            lvl = sensor._req.get_value(sensor.dl)  # heartbeat
            if fired:
                print(f"[MOTION] DL={1 if lvl else 0}")
                sensor.clear_interrupt()
            else:
                print(f"[{time.monotonic()-t0:4.1f}s] DL={1 if lvl else 0} (waiting...)")
    except KeyboardInterrupt:
        pass

def main():
    ap = argparse.ArgumentParser(description="PYD1588 demo")
    ap.add_argument("--chip", default="/dev/gpiochip4", help="gpiochip path (default: /dev/gpiochip4)")
    ap.add_argument("--dl", type=int, default=17, help="DL GPIO offset (default: 17)")
    ap.add_argument("--serin", type=int, default=27, help="SERIN GPIO offset (default: 27)")
    ap.add_argument("--mode", choices=["raw", "wakeup"], default="raw", help="raw amplitude or wakeup motion")
    ap.add_argument("--rate", type=float, default=40.0, help="raw mode sample rate Hz (default: 40)")
    args = ap.parse_args()

    sensor = PYD1588(gpiochip=args.chip, dl=args.dl, serin=args.serin)

    try:
        if args.mode == "raw":
            run_raw(sensor, rate_hz=args.rate)
        else:
            run_wakeup(sensor)
    finally:
        sensor.close()

if __name__ == "__main__":
    main()

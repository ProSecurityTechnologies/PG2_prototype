
"""
Example app for PYD1588 on Raspberry Pi 5 / CM5 using libgpiod.
- Configures the sensor
- Arms Wake-Up (interrupt) mode
- Logs motion events and clears the interrupt

Usage:
  python3 main.py [--chip /dev/gpiochip4] [--dl 17] [--serin 27]
                  [--threshold 20] [--blind 3] [--pulses 1] [--wtime 1]
"""
from __future__ import annotations
import argparse
import time
from pir_pyd1588 import PYD1588, make_config

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--chip", default="/dev/gpiochip4", help="gpiochip path (auto-detect if omitted)")
    p.add_argument("--dl", type=int, default=17, help="GPIO number for DL (default 17)")
    p.add_argument("--serin", type=int, default=10, help="GPIO number for SERIN (default 10)")
    p.add_argument("--threshold", type=int, default=20, help="Motion threshold (0..255)")
    p.add_argument("--blind", type=int, default=3, help="Blind time steps (0..15)")
    p.add_argument("--pulses", type=int, default=1, help="Pulse counter (0..3) -> needs N+1 pulses")
    p.add_argument("--wtime", type=int, default=1, help="Window time (0..3)")
    p.add_argument("--source", type=int, default=0, help="Filter source: 0=BPF 1=LPF")
    p.add_argument("--countmode", type=int, default=0, help="Count mode bit")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()

def main():
    args = parse_args()
    sensor = PYD1588(gpiochip=args.chip, dl=args.dl, serin=args.serin)
    try:
        cfg = make_config(threshold=args.threshold, blind=args.blind,
                          pulses=args.pulses, wtime=args.wtime,
                          opmode=2, countmode=args.countmode,
                          source=args.source)
        if args.verbose:
            print(f"Using gpiochip={sensor.gpiochip or 'auto'} DL=GPIO{sensor.dl} SERIN=GPIO{sensor.serin}")
            print(f"Config word: 0b{cfg:025b} (0x{cfg:06x})")

        sensor.write_config(cfg)
        sensor.arm_wakeup()
        sensor.clear_interrupt()
        print("PYD1588 armed (Wake-Up). Move in front of the sensor... Ctrl+C to stop.")

        while True:
            fired = sensor.wait_for_motion(timeout_s=0.5)
            # lvl = sensor._req.get_value(sensor.dl)
            if fired:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] MOTION")
                sensor.clear_interrupt()
            else:
                if args.verbose:
                    print("(no motion in last 10s)")
    except KeyboardInterrupt:
        pass
    finally:
        sensor.close()

if __name__ == "__main__":
    main()

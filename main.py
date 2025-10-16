import pir_pyd1588
import pigpio
import time

pi = pigpio.pi()
sensor = PYD1588(pi, gpio_dl=17, gpio_serin=27)

cfg = make_config(opmode=0, source=0)  # forced read, BPF
sensor.write_config(cfg)

print("Reading raw ADC (BPF) via Forced Readout. Ctrl+C to stop.")
try:
    while True:
        oor, adc, cfg_read = sensor.forced_read()
        # ADC is 14-bit; for BPF it's signed (−8192..+8191)
        if adc & 0x2000:  # sign bit (bit 13) for BPF source
            adc = adc - 0x4000
        print(f"OOR={oor}  ADC={adc:6d}")
        time.sleep(0.05)  # 20 Hz
except KeyboardInterrupt:
    pass
finally:
    pi.stop()

import pigpio
import time

class PYD1588:
    # Protocol timings (microseconds), per datasheet
    T_SHD = 80        # SERIN data hold >= 80 us
    T_SLT = 700       # SERIN latch low time > 650 us
    T_DS  = 130       # Forced read start: DL high >=120 us
    T_UP  = 1300      # DL low after packet to update regs >1250 us
    T_BIT = 18        # DL bit period <22 us (host toggling cadence)
    T_INTCLR = 200    # Clear interrupt: pull DL low >=160 us

    def __init__(self, pi, gpio_dl=17, gpio_serin=27):
        self.pi = pi
        self.gpio_dl = gpio_dl
        self.gpio_serin = gpio_serin

        # Setup pins
        self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)     # We'll switch modes dynamically
        self.pi.set_pull_up_down(self.gpio_dl, pigpio.PUD_OFF)
        self.pi.write(self.gpio_dl, 0)                    # DL idle low

        self.pi.set_mode(self.gpio_serin, pigpio.OUTPUT)
        self.pi.set_pull_up_down(self.gpio_serin, pigpio.PUD_OFF)
        self.pi.write(self.gpio_serin, 0)

    # --- SERIN: send 25-bit config word MSB first ---
    def write_config(self, cfg25):
        # During configuration, DL must be kept LOW by host
        self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)
        self.pi.write(self.gpio_dl, 0)

        for bit in range(24, -1, -1):  # 25 bits: [24..0]
            b = (cfg25 >> bit) & 1
            # low->high edge, then hold data level >=80us
            self.pi.write(self.gpio_serin, 0)
            # very short tSL allowed, 1 instruction is fine; ensure an edge
            self.pi.write(self.gpio_serin, 1)  # rising edge clocks data
            self.pi.write(self.gpio_serin, b)  # apply data level
            time.sleep(self.T_SHD / 1_000_000.0)

        # Latch by keeping SERIN low gap >650us
        self.pi.write(self.gpio_serin, 0)
        time.sleep(self.T_SLT / 1_000_000.0)
        # Datasheet: config available for readback after ~2.4ms
        time.sleep(0.003)

    # --- Wake-Up mode helper: wait for rising edge on DL, then clear ---
    def wait_for_motion(self, timeout_s=None):
        # DL must be input (Hi-Z) so sensor can drive it
        self.pi.set_mode(self.gpio_dl, pigpio.INPUT)
        self.pi.set_pull_up_down(self.gpio_dl, pigpio.PUD_DOWN)

        cb = self.pi.callback(self.gpio_dl, pigpio.RISING_EDGE)
        start = time.time()
        fired = False
        try:
            while True:
                if cb.tally() > 0:
                    fired = True
                    break
                if timeout_s is not None and (time.time() - start) >= timeout_s:
                    break
                time.sleep(0.001)
        finally:
            cb.cancel()

        if not fired:
            return False

        # Clear the interrupt: DL must be driven LOW >=160us
        self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)
        self.pi.write(self.gpio_dl, 0)
        time.sleep(self.T_INTCLR / 1_000_000.0)
        # Release (Hi-Z) again
        self.pi.set_mode(self.gpio_dl, pigpio.INPUT)
        self.pi.set_pull_up_down(self.gpio_dl, pigpio.PUD_DOWN)
        return True

    # --- Forced read: read 40-bit packet from DL ---
    def forced_read(self):
        # Start condition: DL high >=120us, then low
        self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)
        self.pi.write(self.gpio_dl, 1)
        time.sleep(self.T_DS / 1_000_000.0)
        self.pi.write(self.gpio_dl, 0)

        # Now read bits; host clocks by making a short HI then releasing to input
        bits = 0
        for i in range(40):
            # Host generates LOW->HIGH and releases (input/Hi-Z),
            # sensor drives 0 (pulls low) or leaves high for 1
            self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)
            self.pi.write(self.gpio_dl, 1)  # rising edge
            # release to input quickly so sensor can drive level
            self.pi.set_mode(self.gpio_dl, pigpio.INPUT)
            # allow settle within <22us window
            time.sleep(self.T_BIT / 1_000_000.0)
            val = self.pi.read(self.gpio_dl)
            bits = (bits << 1) | (1 if val else 0)
            # Host pulls low to prep next bit. Keep low very briefly (<22us).
            self.pi.set_mode(self.gpio_dl, pigpio.OUTPUT)
            self.pi.write(self.gpio_dl, 0)
            # small low time to avoid exceeding the 22us spec
            time.sleep(self.T_BIT / 1_000_000.0)

        # Termination: DL low >=1.25ms for register update
        self.pi.write(self.gpio_dl, 0)
        time.sleep(self.T_UP / 1_000_000.0)
        # Release to input again
        self.pi.set_mode(self.gpio_dl, pigpio.INPUT)

        # Parse packet: [39]=OOR, [38:25]=14-bit ADC, [24:0]=config
        oor = (bits >> 39) & 0x1
        adc = (bits >> 25) & 0x3FFF
        cfg = bits & 0x1FFFFFF

        return oor, adc, cfg

# pir_pyd1588.py
"""
PYD1588 driver for Raspberry Pi 5 / CM5 using libgpiod (Python v2 bindings: "gpiod").
Implements configuration via SERIN and Wake-Up (interrupt) mode on DL.
Raw forced readout is not implemented due to <22 µs timing that is unreliable from userspace.
"""

from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional, Dict

try:
    import gpiod  # v2 API (pip install gpiod)
except Exception as e:
    raise RuntimeError(
        "This driver requires the libgpiod v2 Python package 'gpiod'.\n"
        "Install with:  python3 -m pip install --upgrade gpiod"
    ) from e


# ---------- Helpers ----------

def busy_wait_us(us: int) -> None:
    target = time.monotonic_ns() + int(us * 1000)
    while time.monotonic_ns() < target:
        pass


def find_default_gpiochip(preferred: str = "/dev/gpiochip4") -> str:
    """
    Return a likely gpiochip path for the 40-pin header on Pi 5/CM5.
    Defaults to /dev/gpiochip4 if present; otherwise probes chips for line 17.
    """
    try:
        with gpiod.Chip(preferred):
            return preferred
    except Exception:
        pass

    for chipnum in range(0, 8):
        path = f"/dev/gpiochip{chipnum}"
        try:
            with gpiod.Chip(path) as chip:
                req = chip.request_lines(
                    consumer="pyd1588-probe",
                    config={17: gpiod.LineSettings(direction=gpiod.line.Direction.INPUT)},
                )
                req.release()
                return path
        except Exception:
            continue
    return "/dev/gpiochip0"


# ---------- Config register builder ----------

def make_config(
    threshold: int = 20,
    blind: int = 3,
    pulses: int = 1,
    wtime: int = 1,
    opmode: int = 2,
    countmode: int = 0,
    source: int = 0,
) -> int:
    """
    Build the 25-bit configuration word (MSB..LSB).

      [24:17] Threshold (8b)
      [16:13] BlindTime (4b)
      [12:11] PulseCounter (2b)
      [10:9]  WindowTime (2b)
      [8:7]   OpMode (2b): 0=Forced, 1=Interrupt-Readout, 2=Wake-Up (3=reserved)
      [6]     CountMode (1b)
      [5:4]   FilterSource (2b): 0=BPF, 1=LPF, 2/3=vendor-defined
      [3:0]   reserved
    """
    cfg = 0
    cfg |= (threshold & 0xFF) << 17
    cfg |= (blind & 0x0F) << 13
    cfg |= (pulses & 0x03) << 11
    cfg |= (wtime & 0x03) << 9
    cfg |= (opmode & 0x03) << 7
    cfg |= (countmode & 0x01) << 6
    cfg |= (source & 0x03) << 4
    return cfg


# ---------- Driver ----------

@dataclass
class PYD1588:
    """
    Driver for PYD1588 using libgpiod v2.

    SERIN: output-only (configuration)
    DL:    input (interrupt) most of the time; briefly output-low to clear interrupt.
    """
    gpiochip: Optional[str] = None
    dl: int = 17
    serin: int = 27

    # Private handles
    _chip: Optional[gpiod.Chip] = None
    _req: Optional[gpiod.Request] = None

    # Timing constants (microseconds) from datasheet
    T_SHD = 80        # SERIN data hold >= 80 µs
    T_SLT = 700       # SERIN latch low time > 650 µs
    T_INTCLR = 200    # Clear interrupt: DL low >= 160 µs
    T_CFG_SETTLE_MS = 3  # ~2.4 ms until config is available; use 3 ms

    # ----- internal helpers -----

    def _ensure_chip(self) -> None:
        if self._chip is None:
            path = self.gpiochip or find_default_gpiochip()
            self._chip = gpiod.Chip(path)

    @staticmethod
    def _ls(
        direction: gpiod.line.Direction,
        *,
        value: Optional[gpiod.line.Value] = None,
        bias: Optional[gpiod.line.Bias] = None,
        edge: Optional[gpiod.line.Edge] = None,
    ) -> gpiod.LineSettings:
        s = gpiod.LineSettings(direction=direction)
        if value is not None:
            s.output_value = value
        if bias is not None:
            s.bias = bias
        if edge is not None:
            s.edge_detection = edge
        return s

    def _request_lines(self, config: Dict[int, gpiod.LineSettings]) -> None:
        self._ensure_chip()
        if self._req is not None:
            self._req.release()
            self._req = None
        self._req = self._chip.request_lines(consumer="pyd1588", config=config)

    def _reconfigure(self, config: Dict[int, gpiod.LineSettings]) -> None:
        assert self._req is not None, "Lines not requested"
        self._req.reconfigure_lines(config)

    def _set_value_enum(self, offset: int, active: bool) -> None:
        """Always set with the v2 enum (no raw ints)."""
        assert self._req is not None
        self._req.set_value(offset, gpiod.line.Value.ACTIVE if active else gpiod.line.Value.INACTIVE)

    # ----- lifecycle -----

    def close(self) -> None:
        try:
            if self._req is not None:
                self._req.release()
        finally:
            self._req = None
            if self._chip is not None:
                self._chip.close()
            self._chip = None

    # ----- public API -----

    def write_config(self, cfg25: int) -> None:
        """
        Send 25-bit configuration word over SERIN. Keeps DL low during send.
        """
        self._request_lines({
            self.dl: self._ls(gpiod.line.Direction.OUTPUT, value=gpiod.line.Value.INACTIVE),
            self.serin: self._ls(gpiod.line.Direction.OUTPUT, value=gpiod.line.Value.INACTIVE),
        })

        # Send MSB first
        for bit in range(24, -1, -1):
            b = (cfg25 >> bit) & 1
            # Rising edge, then present data level and hold >= 80 µs
            self._set_value_enum(self.serin, False)  # 0
            self._set_value_enum(self.serin, True)   # 1 (edge)
            self._set_value_enum(self.serin, bool(b))
            busy_wait_us(self.T_SHD)

        # Latch with low gap > 650 µs
        self._set_value_enum(self.serin, False)
        busy_wait_us(self.T_SLT)

        # Settle
        time.sleep(self.T_CFG_SETTLE_MS / 1000.0)

    def arm_wakeup(self) -> None:
        """
        Configure DL for rising-edge interrupt detection (Wake-Up mode).
        Leaves SERIN as output low.
        """
        if self._req is None:
            self._request_lines({
                self.dl: self._ls(
                    gpiod.line.Direction.INPUT,
                    bias=gpiod.line.Bias.PULL_DOWN,
                    edge=gpiod.line.Edge.RISING,
                ),
                self.serin: self._ls(
                    gpiod.line.Direction.OUTPUT, value=gpiod.line.Value.INACTIVE
                ),
            })
        else:
            self._reconfigure({
                self.dl: self._ls(
                    gpiod.line.Direction.INPUT,
                    bias=gpiod.line.Bias.PULL_DOWN,
                    edge=gpiod.line.Edge.RISING,
                )
            })

    def wait_for_motion(self, timeout_s: Optional[float] = None) -> bool:
        """
        Block until a motion event occurs or timeout elapses. Returns True if fired.
        Call arm_wakeup() first.
        """
        assert self._req is not None, "Call arm_wakeup() first"
        timeout_ms = None if timeout_s is None else int(timeout_s * 1000)
        evt = self._req.read_edge_events(self.dl)
        return evt is not None

    def clear_interrupt(self) -> None:
        """
        Drive DL low >= 160 µs to clear Wake-Up interrupt, then re-arm input.
        """
        assert self._req is not None, "arm_wakeup() must be called first"

        # Temporarily drive DL low
        self._reconfigure({
            self.dl: self._ls(gpiod.line.Direction.OUTPUT, value=gpiod.line.Value.INACTIVE)
        })
        self._set_value_enum(self.dl, False)
        busy_wait_us(self.T_INTCLR)

        # Re-arm rising edge detection with pulldown
        self._reconfigure({
            self.dl: self._ls(
                gpiod.line.Direction.INPUT,
                bias=gpiod.line.Bias.PULL_DOWN,
                edge=gpiod.line.Edge.RISING,
            )
        })

    def forced_read(self):
        raise NotImplementedError(
            "Forced/interrupt readout requires <22 µs bit timing on DL, which is "
            "not reliable from userspace Python on Pi 5. Use Wake-Up mode or an "
            "external microcontroller for raw data."
        )

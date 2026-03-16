#!/usr/bin/env python3
from __future__ import annotations

import math
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

try:
    import psutil
except ImportError:
    print("Missing dependency: psutil")
    print("Install it with: python3 -m pip install psutil")
    sys.exit(1)


# ------------------------------- Config ------------------------------------ #

REFRESH_RATE = 1.0
FRAME_RATE = 18.0
ENABLE_COLOR = True
COMPACT_MODE = False
ROBOT_SPEED = 18.0
STARFIELD_ENABLED = True


# ------------------------------- ANSI Theme -------------------------------- #

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

ANSI = {
    "reset": RESET,
    "none": "",
    "bg": "\033[48;5;16m",
    "panel_bg": "\033[48;5;17m",
    "title": "\033[1;38;5;213m",
    "brand": "\033[1;38;5;219m",
    "accent": "\033[1;38;5;87m",
    "accent2": "\033[1;38;5;141m",
    "mint": "\033[1;38;5;121m",
    "gold": "\033[1;38;5;221m",
    "warn": "\033[1;38;5;215m",
    "bad": "\033[1;38;5;203m",
    "good": "\033[1;38;5;121m",
    "info": "\033[38;5;117m",
    "muted": "\033[38;5;245m",
    "dim": "\033[2;38;5;240m",
    "border": "\033[38;5;60m",
    "border_hot": "\033[38;5;177m",
    "panel_title": "\033[1;38;5;225m",
    "label": "\033[38;5;153m",
    "value": "\033[38;5;255m",
    "bar_empty": "\033[38;5;238m",
    "bar_good": "\033[38;5;121m",
    "bar_warn": "\033[38;5;221m",
    "bar_hot": "\033[38;5;215m",
    "bar_bad": "\033[38;5;203m",
    "star_a": "\033[2;38;5;60m",
    "star_b": "\033[2;38;5;111m",
    "star_c": "\033[2;38;5;177m",
    "robot": "\033[1;38;5;213m",
    "robot_eye": "\033[1;38;5;87m",
    "robot_trim": "\033[1;38;5;121m",
    "highlight": "\033[1;38;5;87m",
    "alert": "\033[1;38;5;203m",
    "footer": "\033[38;5;153m",
}

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
HEAVY_APPS = {
    "windowserver",
    "google chrome",
    "chrome",
    "codex",
    "iterm2",
    "itermserver-",
    "node",
    "python",
    "python3",
    "code",
    "visual studio code",
    "safari",
}


# ------------------------------- Models ------------------------------------ #


@dataclass
class AppConfig:
    refresh_rate: float = REFRESH_RATE
    frame_rate: float = FRAME_RATE
    enable_color: bool = ENABLE_COLOR
    compact_mode: bool = COMPACT_MODE
    robot_speed: float = ROBOT_SPEED
    starfield_enabled: bool = STARFIELD_ENABLED


@dataclass
class BatteryDetails:
    percent: float | None = None
    plugged: bool | None = None
    secsleft: int | None = None
    source: str | None = None
    health_percent: str | None = None
    cycle_count: str | None = None
    condition: str | None = None
    voltage_mv: int | None = None
    amperage_ma: int | None = None
    wattage_w: float | None = None
    temperature_c: float | None = None
    current_capacity_mah: int | None = None
    max_capacity_mah: int | None = None
    design_capacity_mah: int | None = None


@dataclass
class ProcessInfo:
    pid: int
    name: str
    cpu_percent: float
    highlight: bool = False


@dataclass
class MetricsSnapshot:
    cpu_total: float = 0.0
    per_core: list[float] = field(default_factory=list)
    load_avg: tuple[float, float, float] = (0.0, 0.0, 0.0)
    core_count: int = 0
    memory_used: int = 0
    memory_total: int = 0
    memory_available: int = 0
    memory_free: int = 0
    memory_percent: float = 0.0
    swap_used: int = 0
    swap_total: int = 0
    swap_percent: float = 0.0
    disk_used: int = 0
    disk_total: int = 0
    disk_percent: float = 0.0
    disk_read_rate: float = 0.0
    disk_write_rate: float = 0.0
    net_up_rate: float = 0.0
    net_down_rate: float = 0.0
    local_ip: str = "N/A"
    battery: BatteryDetails = field(default_factory=BatteryDetails)
    top_processes: list[ProcessInfo] = field(default_factory=list)
    alerts: list[str] = field(default_factory=list)
    status: str = "Stable"


@dataclass
class StaticInfo:
    hostname: str
    chip: str
    os_label: str
    machine: str
    ram_total: int
    disk_total: int


@dataclass
class Layout:
    width: int
    height: int
    compact: bool
    split: bool
    header_h: int
    footer_h: int
    robot_h: int
    body_y: int
    left_x: int
    right_x: int
    col_w: int
    left_h: int
    right_h: int


# ------------------------------- Utilities --------------------------------- #


def style(name: str, config: AppConfig) -> str:
    if not config.enable_color:
        return ""
    return ANSI.get(name, "")


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def visible_width(text: str) -> int:
    return len(strip_ansi(text))


def truncate_plain(text: str, width: int, ellipsis: str = "…") -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width == 1:
        return ellipsis
    return text[: width - 1] + ellipsis


def ansi_truncate(text: str, width: int) -> str:
    plain = strip_ansi(text)
    clipped = truncate_plain(plain, width)
    return clipped


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_percent(value: float | None) -> float:
    try:
        if value is None or math.isnan(float(value)):
            return 0.0
        return clamp(float(value), 0.0, 999.0)
    except (TypeError, ValueError):
        return 0.0


def format_bytes(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    number = float(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for unit in units:
        if abs(number) < 1024.0 or unit == units[-1]:
            return f"{number:.1f} {unit}"
        number /= 1024.0
    return f"{number:.1f} PB"


def format_rate(value: float | int | None) -> str:
    return f"{format_bytes(value)}/s"


def format_watts(value: float | None) -> str:
    if value is None:
        return "N/A"
    sign = "+" if value > 0.05 else "-" if value < -0.05 else ""
    return f"{sign}{abs(value):.1f} W"


def format_millivolts(value: int | None) -> str:
    if value is None:
        return "N/A"
    return f"{value / 1000.0:.2f} V"


def format_celsius(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1f} C"


def format_percent(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{safe_percent(value):.1f}%"


def format_secs(seconds: int | None) -> str:
    if seconds is None or seconds < 0:
        return "N/A"
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    return f"{hours:d}h {minutes:02d}m"


def header_power_label(battery: BatteryDetails) -> str:
    if battery.health_percent:
        return f"Health {battery.health_percent}"
    if battery.percent is not None:
        return f"Power {battery.percent:.0f}%"
    return "Power N/A"


def valid_health_percent(value: str | None) -> bool:
    if not value:
        return False
    try:
        numeric = float(value.replace("%", "").strip())
    except ValueError:
        return False
    return 40.0 <= numeric <= 110.0


def safe_run(command: list[str], timeout: float = 2.0) -> str | None:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def local_ip() -> str:
    try:
        for addrs in psutil.net_if_addrs().values():
            for addr in addrs:
                if (
                    getattr(addr, "family", None) == socket.AF_INET
                    and addr.address
                    and not addr.address.startswith("127.")
                ):
                    return addr.address
    except Exception:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.0.2.1", 80))
            return sock.getsockname()[0]
    except OSError:
        return "N/A"


def current_load() -> tuple[float, float, float]:
    try:
        return os.getloadavg()
    except (AttributeError, OSError):
        return (0.0, 0.0, 0.0)


def chip_label() -> str:
    if platform.system() == "Darwin":
        hardware = safe_run(["system_profiler", "SPHardwareDataType"], timeout=4.0)
        if hardware:
            for line in hardware.splitlines():
                stripped = line.strip()
                if stripped.startswith("Chip:"):
                    return stripped.split(":", 1)[1].strip()
                if stripped.startswith("Processor Name:"):
                    return stripped.split(":", 1)[1].strip()
    for command in (
        ["sysctl", "-n", "machdep.cpu.brand_string"],
        ["sysctl", "-n", "hw.model"],
    ):
        value = safe_run(command)
        if value:
            return value
    processor = platform.processor() or platform.machine() or "CPU"
    return processor


def os_label() -> str:
    version, _, _ = platform.mac_ver()
    if version:
        return f"macOS {version}"
    return f"{platform.system()} {platform.release()}"


def severity_style(percent: float) -> str:
    pct = safe_percent(percent)
    if pct >= 90:
        return "bar_bad"
    if pct >= 75:
        return "bar_hot"
    if pct >= 55:
        return "bar_warn"
    return "bar_good"


def text_severity_style(percent: float) -> str:
    pct = safe_percent(percent)
    if pct >= 90:
        return "bad"
    if pct >= 75:
        return "warn"
    if pct >= 55:
        return "gold"
    return "mint"


def meter(percent: float, width: int = 18) -> str:
    partials = "▏▎▍▌▋▊▉"
    pct = clamp(safe_percent(percent), 0.0, 100.0)
    scaled = (pct / 100.0) * width
    full = int(scaled)
    remainder = scaled - full
    bar = ["█"] * full
    if full < width and remainder > 0:
        index = min(len(partials) - 1, max(0, math.ceil(remainder * len(partials)) - 1))
        bar.append(partials[index])
    while len(bar) < width:
        bar.append("░")
    return "".join(bar[:width])


def safe_value(callable_obj, default):
    try:
        return callable_obj()
    except Exception:
        return default


def compact_label(name: str, width: int) -> str:
    clean = name.strip() or "unknown"
    return truncate_plain(clean, width)


def safe_process_iter(attrs: list[str]) -> list[psutil.Process]:
    try:
        return list(psutil.process_iter(attrs))
    except (psutil.Error, PermissionError, OSError):
        return []


def robot_frames(direction: int) -> list[list[tuple[str, str]]]:
    right = [
        [
            ("    /\\_/\\\\     ", "robot_trim"),
            (" __((◉ ◉ ))>   ", "robot_eye"),
            ("/__/{▣▣▣}\\\\    ", "robot"),
            ("    /  \\\\      ", "robot_trim"),
        ],
        [
            ("    /\\_/\\\\     ", "robot_trim"),
            (" __((◉ ◉ ))>   ", "robot_eye"),
            ("/__/{▣▣▣}\\\\    ", "robot"),
            ("     /\\\\       ", "robot_trim"),
        ],
        [
            ("    /\\_/\\\\     ", "robot_trim"),
            (" __((◉ ◉ ))>   ", "robot_eye"),
            ("/__/{▣▣▣}\\\\    ", "robot"),
            ("    _/ \\\\      ", "robot_trim"),
        ],
        [
            ("    /\\_/\\\\     ", "robot_trim"),
            (" __((◉ ◉ ))>   ", "robot_eye"),
            ("/__/{▣▣▣}\\\\    ", "robot"),
            ("    /_/\\\\      ", "robot_trim"),
        ],
    ]
    left = [
        [
            ("     //\\_/\\\\   ", "robot_trim"),
            ("   <(( ◉ ◉))__ ", "robot_eye"),
            ("    //{▣▣▣}\\__\\", "robot"),
            ("      //  \\\\   ", "robot_trim"),
        ],
        [
            ("     //\\_/\\\\   ", "robot_trim"),
            ("   <(( ◉ ◉))__ ", "robot_eye"),
            ("    //{▣▣▣}\\__\\", "robot"),
            ("       //\\\\    ", "robot_trim"),
        ],
        [
            ("     //\\_/\\\\   ", "robot_trim"),
            ("   <(( ◉ ◉))__ ", "robot_eye"),
            ("    //{▣▣▣}\\__\\", "robot"),
            ("      // \\_    ", "robot_trim"),
        ],
        [
            ("     //\\_/\\\\   ", "robot_trim"),
            ("   <(( ◉ ◉))__ ", "robot_eye"),
            ("    //{▣▣▣}\\__\\", "robot"),
            ("      //\\\\_    ", "robot_trim"),
        ],
    ]
    return right if direction >= 0 else left


# ------------------------------- Canvas ------------------------------------ #


class Canvas:
    def __init__(self, width: int, height: int, config: AppConfig) -> None:
        self.width = max(1, width)
        self.height = max(1, height)
        self.config = config
        self.chars = [[" "] * self.width for _ in range(self.height)]
        self.styles = [["bg"] * self.width for _ in range(self.height)]

    def put(self, x: int, y: int, ch: str, style_name: str = "value") -> None:
        if 0 <= x < self.width and 0 <= y < self.height and ch:
            self.chars[y][x] = ch[0]
            self.styles[y][x] = style_name

    def draw_text(
        self, x: int, y: int, text: str, style_name: str = "value", max_width: int | None = None
    ) -> None:
        if y < 0 or y >= self.height:
            return
        if max_width is None:
            max_width = self.width - x
        if max_width <= 0:
            return
        clipped = truncate_plain(text, max_width)
        for index, ch in enumerate(clipped):
            self.put(x + index, y, ch, style_name)

    def draw_segments(
        self, x: int, y: int, segments: Iterable[tuple[str, str]], max_width: int | None = None
    ) -> None:
        if y < 0 or y >= self.height:
            return
        cursor = x
        remaining = self.width - x if max_width is None else max_width
        if remaining <= 0:
            return
        for text, style_name in segments:
            if remaining <= 0:
                break
            clipped = truncate_plain(text, remaining)
            self.draw_text(cursor, y, clipped, style_name, remaining)
            width = len(clipped)
            cursor += width
            remaining -= width

    def draw_hline(
        self, x: int, y: int, width: int, ch: str = "─", style_name: str = "border"
    ) -> None:
        for offset in range(max(0, width)):
            self.put(x + offset, y, ch, style_name)

    def draw_box(
        self, x: int, y: int, width: int, height: int, title: str | None = None, hot: bool = False
    ) -> None:
        if width < 2 or height < 2:
            return
        border_style = "border_hot" if hot else "border"
        self.put(x, y, "╭", border_style)
        self.put(x + width - 1, y, "╮", border_style)
        self.put(x, y + height - 1, "╰", border_style)
        self.put(x + width - 1, y + height - 1, "╯", border_style)
        self.draw_hline(x + 1, y, width - 2, "─", border_style)
        self.draw_hline(x + 1, y + height - 1, width - 2, "─", border_style)
        for row in range(y + 1, y + height - 1):
            self.put(x, row, "│", border_style)
            for col in range(x + 1, x + width - 1):
                self.put(col, row, " ", "panel_bg")
            self.put(x + width - 1, row, "│", border_style)
        if title:
            label = f" {title} "
            self.draw_text(x + 2, y, truncate_plain(label, max(1, width - 4)), "panel_title")

    def render(self) -> str:
        lines = []
        reset_code = style("reset", self.config)
        for row_index in range(self.height):
            row_chars = self.chars[row_index]
            row_styles = self.styles[row_index]
            current_style = None
            parts: list[str] = []
            for char, style_name in zip(row_chars, row_styles):
                code = style(style_name, self.config)
                if code != current_style:
                    parts.append(code)
                    current_style = code
                parts.append(char)
            if current_style:
                parts.append(reset_code)
            lines.append("".join(parts))
        return "\033[H" + "\n".join(lines) + reset_code


# ------------------------------- Sampling ---------------------------------- #


class MetricSampler:
    def __init__(self) -> None:
        vm = safe_value(psutil.virtual_memory, None)
        disk = safe_value(lambda: psutil.disk_usage("/"), None)
        self.static = StaticInfo(
            hostname=platform.node() or "Fitz-Mac",
            chip=chip_label(),
            os_label=os_label(),
            machine=platform.machine() or "macOS",
            ram_total=getattr(vm, "total", 0),
            disk_total=getattr(disk, "total", 0),
        )
        self.last_sample = time.monotonic()
        self.prev_disk = safe_value(psutil.disk_io_counters, None)
        self.prev_net = safe_value(psutil.net_io_counters, None)
        self.cached_ip = local_ip()
        self.next_ip_refresh = 0.0
        self.cached_battery_extra = BatteryDetails()
        self.next_battery_refresh = 0.0
        safe_value(lambda: psutil.cpu_percent(interval=None), 0.0)
        safe_value(lambda: psutil.cpu_percent(interval=None, percpu=True), [])
        for proc in safe_process_iter(["pid", "name"]):
            try:
                proc.cpu_percent(None)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

    def sample(self) -> MetricsSnapshot:
        now = time.monotonic()
        elapsed = max(0.001, now - self.last_sample)
        self.last_sample = now

        cpu_total = safe_percent(safe_value(lambda: psutil.cpu_percent(interval=None), 0.0))
        per_core = [
            safe_percent(value)
            for value in safe_value(lambda: psutil.cpu_percent(interval=None, percpu=True), [])
        ]
        load_avg = current_load()
        core_count = psutil.cpu_count(logical=True) or len(per_core) or 0

        vm = safe_value(psutil.virtual_memory, None)
        swap = safe_value(psutil.swap_memory, None)
        disk_usage = safe_value(lambda: psutil.disk_usage("/"), None)
        disk_io = safe_value(psutil.disk_io_counters, None)
        net_io = safe_value(psutil.net_io_counters, None)

        disk_read_rate = 0.0
        disk_write_rate = 0.0
        if self.prev_disk and disk_io:
            disk_read_rate = (
                max(0.0, float(disk_io.read_bytes - self.prev_disk.read_bytes)) / elapsed
            )
            disk_write_rate = (
                max(0.0, float(disk_io.write_bytes - self.prev_disk.write_bytes)) / elapsed
            )
        self.prev_disk = disk_io

        net_down_rate = 0.0
        net_up_rate = 0.0
        if self.prev_net and net_io:
            net_down_rate = max(0.0, float(net_io.bytes_recv - self.prev_net.bytes_recv)) / elapsed
            net_up_rate = max(0.0, float(net_io.bytes_sent - self.prev_net.bytes_sent)) / elapsed
        self.prev_net = net_io

        if now >= self.next_ip_refresh:
            self.cached_ip = local_ip()
            self.next_ip_refresh = now + 30.0

        battery = self._battery(now)
        processes = self._top_processes(limit=8)
        alerts = self._alerts(
            cpu_total, getattr(swap, "percent", 0.0), getattr(disk_usage, "percent", 0.0), battery
        )

        return MetricsSnapshot(
            cpu_total=cpu_total,
            per_core=per_core,
            load_avg=load_avg,
            core_count=core_count,
            memory_used=getattr(vm, "used", 0),
            memory_total=getattr(vm, "total", 0),
            memory_available=getattr(vm, "available", 0),
            memory_free=getattr(vm, "free", 0),
            memory_percent=safe_percent(getattr(vm, "percent", 0.0)),
            swap_used=getattr(swap, "used", 0),
            swap_total=getattr(swap, "total", 0),
            swap_percent=safe_percent(getattr(swap, "percent", 0.0)),
            disk_used=getattr(disk_usage, "used", 0),
            disk_total=getattr(disk_usage, "total", 0),
            disk_percent=safe_percent(getattr(disk_usage, "percent", 0.0)),
            disk_read_rate=disk_read_rate,
            disk_write_rate=disk_write_rate,
            net_up_rate=net_up_rate,
            net_down_rate=net_down_rate,
            local_ip=self.cached_ip,
            battery=battery,
            top_processes=processes,
            alerts=alerts,
            status=self._status_label(alerts, battery),
        )

    def _battery(self, now: float) -> BatteryDetails:
        battery = BatteryDetails()
        ps_battery = safe_value(psutil.sensors_battery, None)
        if ps_battery:
            battery.percent = safe_percent(getattr(ps_battery, "percent", None))
            battery.plugged = bool(getattr(ps_battery, "power_plugged", False))
            battery.source = "AC" if battery.plugged else "Battery"
            secsleft = getattr(ps_battery, "secsleft", None)
            battery.secsleft = secsleft if isinstance(secsleft, int) and secsleft >= 0 else None
        if battery.percent is None or battery.secsleft is None:
            battery = self._merge_battery_pmset(battery)
        if now >= self.next_battery_refresh:
            self.cached_battery_extra = self._battery_extra()
            self.next_battery_refresh = now + 300.0
        extra = self.cached_battery_extra
        if extra.health_percent:
            battery.health_percent = extra.health_percent
        if extra.cycle_count:
            battery.cycle_count = extra.cycle_count
        if extra.condition:
            battery.condition = extra.condition
        return battery

    def _battery_extra(self) -> BatteryDetails:
        if platform.system() != "Darwin":
            return BatteryDetails()
        ioreg_details = self._battery_from_ioreg()
        profiler_details = self._battery_from_system_profiler()
        details = BatteryDetails()
        for field_name in (
            "source",
            "voltage_mv",
            "amperage_ma",
            "wattage_w",
            "temperature_c",
            "current_capacity_mah",
            "max_capacity_mah",
            "design_capacity_mah",
        ):
            value = getattr(ioreg_details, field_name)
            if value is not None:
                setattr(details, field_name, value)
        details.health_percent = (
            ioreg_details.health_percent
            if valid_health_percent(ioreg_details.health_percent)
            else profiler_details.health_percent
        )
        details.cycle_count = ioreg_details.cycle_count or profiler_details.cycle_count
        details.condition = ioreg_details.condition or profiler_details.condition
        details.percent = ioreg_details.percent or profiler_details.percent
        return details

    def _merge_battery_pmset(self, battery: BatteryDetails) -> BatteryDetails:
        if platform.system() != "Darwin":
            return battery
        output = safe_run(["pmset", "-g", "batt"], timeout=2.0)
        if not output:
            return battery
        for line in output.splitlines():
            match = re.search(r"(\d+)%", line)
            if match and battery.percent is None:
                battery.percent = safe_percent(match.group(1))
            if "AC Power" in line:
                battery.plugged = True
                battery.source = "AC"
            elif "Battery Power" in line:
                battery.plugged = False
                battery.source = "Battery"
            time_match = re.search(r"(\d+):(\d+)\s+remaining", line)
            if time_match and battery.secsleft is None:
                hours = int(time_match.group(1))
                minutes = int(time_match.group(2))
                battery.secsleft = hours * 3600 + minutes * 60
        return battery

    def _battery_from_ioreg(self) -> BatteryDetails:
        output = safe_run(["ioreg", "-r", "-c", "AppleSmartBattery", "-l"], timeout=2.5)
        if not output:
            return BatteryDetails()
        details = BatteryDetails()
        int_patterns = {
            "current_capacity_mah": r'"CurrentCapacity"\s*=\s*([0-9]+)',
            "max_capacity_mah": r'"MaxCapacity"\s*=\s*([0-9]+)',
            "design_capacity_mah": r'"DesignCapacity"\s*=\s*([0-9]+)',
            "voltage_mv": r'"Voltage"\s*=\s*([0-9]+)',
            "amperage_ma": r'"Amperage"\s*=\s*(-?[0-9]+)',
        }
        text_patterns = {
            "health_percent": r'"MaximumCapacityPercent"\s*=\s*([0-9]+)',
            "cycle_count": r'"CycleCount"\s*=\s*([0-9]+)',
            "condition": r'"BatteryHealthCondition"\s*=\s*"([^"]+)"',
        }
        for field_name, pattern in int_patterns.items():
            match = re.search(pattern, output)
            if match:
                setattr(details, field_name, int(match.group(1).strip()))
        for field_name, pattern in text_patterns.items():
            match = re.search(pattern, output)
            if match:
                value = match.group(1).strip()
                setattr(
                    details, field_name, f"{value}%" if field_name == "health_percent" else value
                )
        source_match = re.search(r'"ExternalConnected"\s*=\s*(Yes|No)', output)
        if source_match:
            details.source = "AC" if source_match.group(1) == "Yes" else "Battery"
        temp_match = re.search(r'"Temperature"\s*=\s*([0-9]+)', output)
        if temp_match:
            raw_temp = int(temp_match.group(1))
            celsius = raw_temp / 100.0
            if 0.0 < celsius < 100.0:
                details.temperature_c = celsius
        if details.amperage_ma is not None and details.voltage_mv is not None:
            details.wattage_w = (details.amperage_ma * details.voltage_mv) / 1_000_000.0
        if not details.health_percent and details.max_capacity_mah and details.design_capacity_mah:
            if details.design_capacity_mah > 0:
                details.health_percent = (
                    f"{(details.max_capacity_mah / details.design_capacity_mah) * 100:.0f}%"
                )
        return details

    def _battery_from_system_profiler(self) -> BatteryDetails:
        output = safe_run(["system_profiler", "SPPowerDataType"], timeout=4.0)
        if not output:
            return BatteryDetails()
        details = BatteryDetails()
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("Maximum Capacity:"):
                details.health_percent = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("Cycle Count:"):
                details.cycle_count = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("Condition:"):
                details.condition = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("Full Charge Capacity (mAh):"):
                try:
                    details.max_capacity_mah = int(stripped.split(":", 1)[1].strip())
                except ValueError:
                    pass
            elif stripped.startswith("State of Charge (%):") and not details.percent:
                try:
                    details.percent = safe_percent(stripped.split(":", 1)[1].strip())
                except ValueError:
                    pass
        return details

    def _top_processes(self, limit: int) -> list[ProcessInfo]:
        processes: list[ProcessInfo] = []
        for proc in safe_process_iter(["pid", "name", "cpu_percent"]):
            try:
                info = proc.info
                cpu_value = safe_percent(info.get("cpu_percent"))
                name = (info.get("name") or f"pid-{info.get('pid', '?')}").strip() or "unknown"
                lowered = name.lower()
                highlight = any(tag in lowered for tag in HEAVY_APPS)
                processes.append(
                    ProcessInfo(
                        pid=int(info.get("pid") or 0),
                        name=name,
                        cpu_percent=cpu_value,
                        highlight=highlight,
                    )
                )
            except (
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                psutil.ZombieProcess,
                ValueError,
                TypeError,
            ):
                continue
        if not processes:
            processes = self._top_processes_ps(limit)
        processes.sort(key=lambda item: (-item.cpu_percent, item.name.lower(), item.pid))
        return processes[:limit]

    def _top_processes_ps(self, limit: int) -> list[ProcessInfo]:
        output = safe_run(["ps", "-Ao", "pid=,pcpu=,comm="], timeout=2.5)
        if not output:
            return []
        processes: list[ProcessInfo] = []
        for line in output.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            pid_text, cpu_text, command = parts
            try:
                pid = int(pid_text)
                cpu_value = safe_percent(cpu_text)
            except (TypeError, ValueError):
                continue
            name = os.path.basename(command.strip()) or command.strip() or f"pid-{pid}"
            lowered = name.lower()
            processes.append(
                ProcessInfo(
                    pid=pid,
                    name=name,
                    cpu_percent=cpu_value,
                    highlight=any(tag in lowered for tag in HEAVY_APPS),
                )
            )
        processes.sort(key=lambda item: (-item.cpu_percent, item.name.lower(), item.pid))
        return processes[:limit]

    def _alerts(
        self, cpu_total: float, swap_percent: float, disk_percent: float, battery: BatteryDetails
    ) -> list[str]:
        alerts: list[str] = []
        if cpu_total >= 80:
            alerts.append(f"CPU {cpu_total:.0f}%")
        if safe_percent(swap_percent) >= 35:
            alerts.append(f"Swap {safe_percent(swap_percent):.0f}%")
        if safe_percent(disk_percent) >= 85:
            alerts.append(f"Disk {safe_percent(disk_percent):.0f}%")
        if battery.percent is not None and not battery.plugged and battery.percent <= 20:
            alerts.append(f"Battery {battery.percent:.0f}%")
        return alerts

    def _status_label(self, alerts: list[str], battery: BatteryDetails) -> str:
        if alerts:
            return "Under Load"
        if battery.percent is not None:
            if battery.plugged:
                return "Charging"
            return "Battery"
        return "Stable"


# ------------------------------- Layout ------------------------------------ #


def build_layout(width: int, height: int, config: AppConfig) -> Layout:
    compact = config.compact_mode or width < 110 or height < 32
    split = not compact and width >= 110 and height >= 30
    header_h = 7 if split else 6
    footer_h = 2
    robot_h = 0
    body_y = header_h
    usable_h = max(8, height - header_h - footer_h - robot_h)
    if split:
        col_gap = 2
        col_w = max(40, (width - 4 - col_gap) // 2)
        left_x = 1
        right_x = left_x + col_w + col_gap
        left_h = usable_h
        right_h = usable_h
    else:
        col_w = width - 2
        left_x = 1
        right_x = 1
        left_h = usable_h
        right_h = 0
    return Layout(
        width=width,
        height=height,
        compact=compact,
        split=split,
        header_h=header_h,
        footer_h=footer_h,
        robot_h=robot_h,
        body_y=body_y,
        left_x=left_x,
        right_x=right_x,
        col_w=col_w,
        left_h=left_h,
        right_h=right_h,
    )


# ------------------------------- Rendering --------------------------------- #


def draw_starfield(canvas: Canvas, tick: float, density: int = 54, top_skip: int = 0) -> None:
    if not canvas.config.starfield_enabled or canvas.width < 50 or canvas.height < 18:
        return
    count = max(18, min(120, (canvas.width * canvas.height) // density))
    span_y = max(1, canvas.height - max(2, top_skip))
    for idx in range(count):
        speed = 0.55 + (idx % 7) * 0.12
        x = int((idx * 19 + tick * speed * 7) % max(1, canvas.width))
        y = top_skip + ((idx * 11 + idx * idx * 3) % span_y)
        char = "·" if idx % 5 else "✦"
        if idx % 11 == 0:
            char = "⋆"
        style_name = ("star_a", "star_b", "star_c")[idx % 3]
        canvas.put(x, y, char, style_name)


def draw_header(
    canvas: Canvas, layout: Layout, static: StaticInfo, snapshot: MetricsSnapshot
) -> None:
    now = datetime.now()
    top_right = now.strftime("%a %b %d %Y  %I:%M:%S %p")
    machine_name = truncate_plain(static.hostname.split(".", 1)[0] or static.hostname, 18)
    summary = f"{machine_name}  ·  {truncate_plain(static.chip, 24)}  ·  {format_bytes(static.ram_total)}/{format_bytes(snapshot.disk_total)}  ·  {static.os_label}"
    power_chip = header_power_label(snapshot.battery)
    cycle_chip = (
        f"{snapshot.battery.cycle_count} cycles" if snapshot.battery.cycle_count else "cycles N/A"
    )
    lower_line = (
        f"FitzBot  ·  {snapshot.status}  ·  {power_chip}  ·  {cycle_chip}  ·  {snapshot.local_ip}"
    )
    right_x = max(1, canvas.width - len(top_right) - 1)
    line0_max = max(16, right_x - 3)
    canvas.draw_segments(
        1,
        0,
        [
            ("FITZ STATUS", "title"),
            ("   ", "muted"),
            ("FitzBot", "accent"),
            ("   ", "muted"),
            (truncate_plain(summary, max(10, line0_max - 24)), "muted"),
        ],
        max_width=line0_max,
    )
    canvas.draw_text(right_x, 0, top_right, "mint")
    status_style = "gold" if snapshot.alerts else "mint"
    canvas.draw_text(
        1,
        layout.header_h - 2,
        truncate_plain(lower_line, canvas.width - 3),
        status_style,
        max_width=canvas.width - 3,
    )
    canvas.draw_hline(1, layout.header_h - 1, canvas.width - 2, "─", "border")


def draw_section_heading(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    title: str,
    hot: bool = False,
    icon: str | None = None,
) -> tuple[int, int]:
    border_style = "border_hot" if hot else "border"
    title_style = "alert" if hot else "panel_title"
    label = f"{icon} {title}" if icon else title
    label = f" {label} "
    canvas.draw_text(x, y, truncate_plain(label, width), title_style, max_width=width)
    rule_x = x + min(width, len(label))
    if rule_x < x + width:
        canvas.draw_hline(rule_x, y, x + width - rule_x, "╌", border_style)
    return x, y + 1


def draw_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    hot: bool = False,
    open_style: bool = False,
    icon: str | None = None,
) -> tuple[int, int]:
    if open_style:
        return draw_section_heading(canvas, x, y, width, title, hot=hot, icon=icon)
    canvas.draw_box(x, y, width, height, title=title, hot=hot)
    return x + 2, y + 1


def draw_kv(
    canvas: Canvas, x: int, y: int, label: str, value: str, width: int, value_style: str = "value"
) -> None:
    label_text = truncate_plain(label, max(4, width // 3))
    label_width = min(max(8, len(label_text) + 1), max(8, width // 3))
    canvas.draw_text(x, y, label_text.ljust(label_width), "label", max_width=label_width)
    canvas.draw_text(
        x + label_width,
        y,
        truncate_plain(value, max(0, width - label_width)),
        value_style,
        max_width=max(0, width - label_width),
    )


def draw_dual_kv(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    left_label: str,
    left_value: str,
    right_label: str,
    right_value: str,
    left_style: str = "value",
    right_style: str = "value",
) -> None:
    if width < 26:
        draw_kv(canvas, x, y, left_label, left_value, width, value_style=left_style)
        return
    gap = 2
    left_width = max(10, (width - gap) // 2)
    right_width = max(10, width - left_width - gap)
    draw_kv(canvas, x, y, left_label, left_value, left_width, value_style=left_style)
    draw_kv(
        canvas,
        x + left_width + gap,
        y,
        right_label,
        right_value,
        right_width,
        value_style=right_style,
    )


def draw_metric_bar(
    canvas: Canvas, x: int, y: int, width: int, label: str, percent: float, suffix: str = ""
) -> None:
    usable = max(8, width)
    label_width = min(10, max(6, usable // 5))
    pct_text = format_percent(percent)
    tail = f" {suffix}".rstrip()
    value_width = len(pct_text) + len(tail) + 1
    bar_width = max(8, usable - label_width - value_width - 2)
    canvas.draw_text(
        x, y, truncate_plain(label, label_width).ljust(label_width), "label", max_width=label_width
    )
    bar_style = severity_style(percent)
    empty_x = x + label_width + 1
    canvas.draw_text(empty_x, y, meter(0.0, bar_width), "bar_empty", max_width=bar_width)
    canvas.draw_text(empty_x, y, meter(percent, bar_width), bar_style, max_width=bar_width)
    canvas.draw_text(empty_x + bar_width + 1, y, pct_text, text_severity_style(percent))
    if tail:
        canvas.draw_text(empty_x + bar_width + 1 + len(pct_text), y, tail, "muted")


def panel_lines_available(height: int) -> int:
    return max(0, height - 2)


def draw_cpu_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    compact: bool,
    open_style: bool = False,
) -> None:
    inner_x, inner_y = draw_panel(
        canvas,
        x,
        y,
        width,
        height,
        "CPU",
        hot=snapshot.cpu_total >= 80,
        open_style=open_style,
        icon="◉",
    )
    inner_w = width if open_style else width - 4
    bottom_y = y + height if open_style else y + height - 1
    row = inner_y
    draw_metric_bar(
        canvas, inner_x, row, inner_w, "Total", snapshot.cpu_total, f"{snapshot.core_count} cores"
    )
    row += 1
    load_text = (
        f"{snapshot.load_avg[0]:.2f}  {snapshot.load_avg[1]:.2f}  {snapshot.load_avg[2]:.2f}"
    )
    draw_kv(canvas, inner_x, row, "Load", load_text, inner_w, value_style="value")
    row += 1

    core_values = snapshot.per_core[:]
    if compact:
        max_rows = max(2, panel_lines_available(height) - 4)
        core_values = core_values[: max_rows * 2]
    columns = 2 if inner_w >= 42 else 1
    slot_w = inner_w if columns == 1 else (inner_w - 2) // 2
    bar_w = max(6, slot_w - 10)
    used_rows = 0
    for index, percent in enumerate(core_values):
        col = index % columns
        if col == 0 and index:
            row += 1
            used_rows += 1
        if row >= bottom_y:
            break
        px = inner_x + col * (slot_w + 2)
        label = f"C{index + 1:02d}"
        canvas.draw_text(px, row, label, "label", max_width=4)
        canvas.draw_text(
            px + 4, row, meter(percent, bar_w), severity_style(percent), max_width=bar_w
        )
        canvas.draw_text(
            px + 4 + bar_w + 1, row, f"{percent:4.0f}%", text_severity_style(percent), max_width=5
        )
    remaining = max(0, len(snapshot.per_core) - len(core_values))
    if remaining and row + 1 < bottom_y:
        canvas.draw_text(
            inner_x, row + 1, f"+ {remaining} more cores off-grid", "muted", max_width=inner_w
        )


def draw_memory_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    open_style: bool = False,
) -> None:
    inner_x, inner_y = draw_panel(
        canvas,
        x,
        y,
        width,
        height,
        "Memory",
        hot=snapshot.memory_percent >= 80 or snapshot.swap_percent >= 35,
        open_style=open_style,
        icon="▣",
    )
    inner_w = width if open_style else width - 4
    row = inner_y
    draw_metric_bar(
        canvas,
        inner_x,
        row,
        inner_w,
        "Used",
        snapshot.memory_percent,
        format_bytes(snapshot.memory_used),
    )
    row += 1
    draw_metric_bar(
        canvas,
        inner_x,
        row,
        inner_w,
        "Free",
        100.0 - snapshot.memory_percent,
        format_bytes(snapshot.memory_free),
    )
    row += 1
    draw_kv(canvas, inner_x, row, "Avail", format_bytes(snapshot.memory_available), inner_w)
    row += 1
    if snapshot.swap_total:
        draw_metric_bar(
            canvas,
            inner_x,
            row,
            inner_w,
            "Swap",
            snapshot.swap_percent,
            format_bytes(snapshot.swap_used),
        )
        row += 1
        draw_kv(canvas, inner_x, row, "SwapTot", format_bytes(snapshot.swap_total), inner_w)
    else:
        draw_kv(canvas, inner_x, row, "Swap", "Unavailable", inner_w, value_style="muted")


def draw_disk_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    open_style: bool = False,
) -> None:
    inner_x, inner_y = draw_panel(
        canvas,
        x,
        y,
        width,
        height,
        "Disk",
        hot=snapshot.disk_percent >= 85,
        open_style=open_style,
        icon="▥",
    )
    inner_w = width if open_style else width - 4
    row = inner_y
    draw_metric_bar(
        canvas,
        inner_x,
        row,
        inner_w,
        "Root",
        snapshot.disk_percent,
        format_bytes(snapshot.disk_used),
    )
    row += 1
    draw_kv(canvas, inner_x, row, "Total", format_bytes(snapshot.disk_total), inner_w)
    row += 1
    draw_kv(
        canvas,
        inner_x,
        row,
        "Read",
        format_rate(snapshot.disk_read_rate),
        inner_w,
        value_style="accent",
    )
    row += 1
    draw_kv(
        canvas,
        inner_x,
        row,
        "Write",
        format_rate(snapshot.disk_write_rate),
        inner_w,
        value_style="accent",
    )


def draw_network_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    open_style: bool = False,
) -> None:
    inner_x, inner_y = draw_panel(
        canvas, x, y, width, height, "Network", open_style=open_style, icon="⇅"
    )
    inner_w = width if open_style else width - 4
    row = inner_y
    draw_kv(
        canvas,
        inner_x,
        row,
        "Down",
        format_rate(snapshot.net_down_rate),
        inner_w,
        value_style="accent",
    )
    row += 1
    draw_kv(
        canvas, inner_x, row, "Up", format_rate(snapshot.net_up_rate), inner_w, value_style="accent"
    )
    row += 1
    draw_kv(canvas, inner_x, row, "LocalIP", snapshot.local_ip, inner_w)
    row += 1
    draw_kv(
        canvas,
        inner_x,
        row,
        "Signal",
        "Nominal" if snapshot.local_ip != "N/A" else "Offline",
        inner_w,
        value_style="mint" if snapshot.local_ip != "N/A" else "warn",
    )


def draw_power_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    open_style: bool = False,
) -> None:
    hot = (
        snapshot.battery.percent is not None
        and not snapshot.battery.plugged
        and snapshot.battery.percent <= 20
    )
    inner_x, inner_y = draw_panel(
        canvas, x, y, width, height, "Power", hot=hot, open_style=open_style, icon="◩"
    )
    inner_w = width if open_style else width - 4
    bottom_y = y + height if open_style else y + height - 1
    row = inner_y
    battery = snapshot.battery
    if battery.percent is not None:
        state = battery.source or ("Charging" if battery.plugged else "Battery")
        draw_metric_bar(canvas, inner_x, row, inner_w, "Level", battery.percent, state)
        row += 1
    else:
        draw_kv(
            canvas, inner_x, row, "Power", "Battery data unavailable", inner_w, value_style="muted"
        )
        row += 1
    detail_rows = [
        (
            "Source",
            battery.source
            or ("AC" if battery.plugged else "Battery" if battery.percent is not None else "N/A"),
            "ETA",
            format_secs(battery.secsleft),
            "value",
            "value",
        ),
        (
            "Health",
            battery.health_percent or "N/A",
            "Cycles",
            battery.cycle_count or "N/A",
            "mint" if battery.health_percent else "muted",
            "value" if battery.cycle_count else "muted",
        ),
        (
            "Cond",
            battery.condition or "N/A",
            "Flow",
            format_watts(battery.wattage_w),
            "value" if battery.condition else "muted",
            "accent" if battery.wattage_w is not None else "muted",
        ),
        (
            "Volt",
            format_millivolts(battery.voltage_mv),
            "Temp",
            format_celsius(battery.temperature_c),
            "value" if battery.voltage_mv is not None else "muted",
            "value" if battery.temperature_c is not None else "muted",
        ),
        (
            "Cells",
            (
                f"{battery.current_capacity_mah or 0}/{battery.max_capacity_mah or 0} mAh"
                if battery.current_capacity_mah is not None and battery.max_capacity_mah is not None
                else "N/A"
            ),
            "Design",
            (
                f"{battery.design_capacity_mah} mAh"
                if battery.design_capacity_mah is not None
                else "N/A"
            ),
            (
                "value"
                if battery.current_capacity_mah is not None and battery.max_capacity_mah is not None
                else "muted"
            ),
            "value" if battery.design_capacity_mah is not None else "muted",
        ),
    ]
    for left_label, left_value, right_label, right_value, left_style, right_style in detail_rows:
        if row >= bottom_y:
            break
        draw_dual_kv(
            canvas,
            inner_x,
            row,
            inner_w,
            left_label,
            left_value,
            right_label,
            right_value,
            left_style=left_style,
            right_style=right_style,
        )
        row += 1


def draw_process_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    compact: bool,
    open_style: bool = False,
) -> None:
    hot = bool(snapshot.top_processes and snapshot.top_processes[0].cpu_percent >= 80)
    inner_x, inner_y = draw_panel(
        canvas,
        x,
        y,
        width,
        height,
        "Processes",
        hot=hot,
        open_style=open_style,
        icon="✶",
    )
    inner_w = width if open_style else width - 4
    bottom_y = y + height if open_style else y + height - 1
    row = inner_y
    processes = snapshot.top_processes[: max(3, panel_lines_available(height) - 1)]
    if compact:
        processes = processes[:4]
    if not processes:
        canvas.draw_text(inner_x, row, "Process telemetry unavailable", "muted", max_width=inner_w)
        return
    name_w = max(12, inner_w - 9)
    for proc in processes:
        if row >= bottom_y:
            break
        name_style = "highlight" if proc.highlight else "value"
        canvas.draw_text(
            inner_x,
            row,
            compact_label(proc.name, name_w).ljust(name_w),
            name_style,
            max_width=name_w,
        )
        cpu_style = text_severity_style(proc.cpu_percent)
        canvas.draw_text(
            inner_x + name_w + 1, row, f"{proc.cpu_percent:5.1f}%", cpu_style, max_width=6
        )
        row += 1


def draw_overview_panel(
    canvas: Canvas, x: int, y: int, width: int, height: int, snapshot: MetricsSnapshot
) -> None:
    hot = snapshot.memory_percent >= 80 or snapshot.disk_percent >= 85 or bool(snapshot.alerts)
    inner_x, inner_y = draw_panel(canvas, x, y, width, height, "SYSTEM BUS", hot=hot)
    inner_w = width - 4
    row = inner_y
    lines = [
        (
            "RAM",
            f"{format_percent(snapshot.memory_percent)} used  |  {format_bytes(snapshot.memory_available)} avail",
            "value",
        ),
        (
            "Disk",
            f"{format_percent(snapshot.disk_percent)} used  |  {format_rate(snapshot.disk_read_rate)} r  {format_rate(snapshot.disk_write_rate)} w",
            "accent",
        ),
        (
            "Net",
            f"{format_rate(snapshot.net_down_rate)} down  |  {format_rate(snapshot.net_up_rate)} up",
            "accent",
        ),
    ]
    battery = snapshot.battery
    power_text = "Battery unavailable"
    power_style = "muted"
    if battery.percent is not None:
        state = "Charging" if battery.plugged else "Battery"
        power_text = f"{state} {battery.percent:.0f}%  |  health {battery.health_percent or 'N/A'}"
        power_style = "mint" if battery.plugged else "value"
    lines.append(("Power", power_text, power_style))
    for label, text, value_style in lines[: panel_lines_available(height)]:
        if row >= y + height - 1:
            break
        draw_kv(canvas, inner_x, row, label, text, inner_w, value_style=value_style)
        row += 1


def split_height(total: int, ratios: list[int]) -> list[int]:
    if total <= 0:
        return [0] * len(ratios)
    minimum = 3 if total >= len(ratios) * 3 else 2
    total_ratio = max(1, sum(ratios))
    heights = [max(minimum, total * ratio // total_ratio) for ratio in ratios]
    diff = total - sum(heights)
    index = 0
    while diff != 0 and heights:
        adjustment = 1 if diff > 0 else -1
        candidate = heights[index % len(heights)] + adjustment
        if candidate >= minimum:
            heights[index % len(heights)] = candidate
            diff -= adjustment
        index += 1
        if index > 64:
            break
    return heights


def draw_split_body(canvas: Canvas, layout: Layout, snapshot: MetricsSnapshot) -> None:
    body_top = layout.body_y
    left_heights = split_height(layout.left_h, [8, 5, 6])
    right_heights = split_height(layout.right_h, [5, 4, 8, 4])

    left_y = body_top
    draw_cpu_panel(
        canvas,
        layout.left_x,
        left_y,
        layout.col_w,
        left_heights[0],
        snapshot,
        compact=False,
        open_style=True,
    )
    left_y += left_heights[0]
    draw_disk_panel(
        canvas, layout.left_x, left_y, layout.col_w, left_heights[1], snapshot, open_style=True
    )
    left_y += left_heights[1]
    draw_process_panel(
        canvas,
        layout.left_x,
        left_y,
        layout.col_w,
        left_heights[2],
        snapshot,
        compact=False,
        open_style=True,
    )

    right_y = body_top
    draw_memory_panel(
        canvas, layout.right_x, right_y, layout.col_w, right_heights[0], snapshot, open_style=True
    )
    right_y += right_heights[0]
    draw_network_panel(
        canvas, layout.right_x, right_y, layout.col_w, right_heights[1], snapshot, open_style=True
    )
    right_y += right_heights[1]
    draw_power_panel(
        canvas, layout.right_x, right_y, layout.col_w, right_heights[2], snapshot, open_style=True
    )
    right_y += right_heights[2]
    alerts_h = max(3, right_heights[3])
    draw_alert_panel(
        canvas, layout.right_x, right_y, layout.col_w, alerts_h, snapshot, open_style=True
    )


def draw_alert_panel(
    canvas: Canvas,
    x: int,
    y: int,
    width: int,
    height: int,
    snapshot: MetricsSnapshot,
    open_style: bool = False,
) -> None:
    inner_x, inner_y = draw_panel(
        canvas,
        x,
        y,
        width,
        height,
        "Signal",
        hot=bool(snapshot.alerts),
        open_style=open_style,
        icon="◇",
    )
    inner_w = width if open_style else width - 4
    bottom_y = y + height if open_style else y + height - 1
    row = inner_y
    alerts = snapshot.alerts or [
        snapshot.status,
        "FitzBot crawling the grid...",
        "System channels nominal",
    ]
    for line in alerts[: panel_lines_available(height)]:
        if row >= bottom_y:
            break
        style_name = "alert" if line in snapshot.alerts else "mint"
        canvas.draw_text(inner_x, row, truncate_plain(line, inner_w), style_name, max_width=inner_w)
        row += 1


def draw_single_body(canvas: Canvas, layout: Layout, snapshot: MetricsSnapshot) -> None:
    if layout.left_h >= 12:
        sections = [
            (draw_cpu_panel, {"compact": True}),
            (draw_overview_panel, {}),
            (draw_process_panel, {"compact": True}),
            (draw_alert_panel, {}),
        ]
        heights = split_height(layout.left_h, [5, 4, 4, 3])
    elif layout.left_h >= 9:
        sections = [
            (draw_cpu_panel, {"compact": True}),
            (draw_overview_panel, {}),
            (draw_alert_panel, {}),
        ]
        heights = split_height(layout.left_h, [4, 3, 2])
    else:
        sections = [
            (draw_overview_panel, {}),
            (draw_alert_panel, {}),
        ]
        heights = split_height(layout.left_h, [3, 2])
    current_y = layout.body_y
    for section, section_height in zip(sections, heights):
        drawer, kwargs = section
        (
            drawer(
                canvas, layout.left_x, current_y, layout.col_w, section_height, snapshot, **kwargs
            )
            if kwargs
            else drawer(canvas, layout.left_x, current_y, layout.col_w, section_height, snapshot)
        )
        current_y += section_height


def draw_top_robot(canvas: Canvas, tick: float, layout: Layout) -> None:
    if canvas.width < 56:
        return
    frames = robot_frames(1)
    robot_width = max(len(text) for text, _style in frames[0])
    lane_width = max(24, min(canvas.width // 3, canvas.width - robot_width - 8))
    lane_left = max(2, (canvas.width - robot_width - lane_width) // 2)
    lane_right = min(canvas.width - robot_width - 2, lane_left + lane_width)
    travel = max(2, lane_right - lane_left)
    phase = tick * canvas.config.robot_speed / max(10.0, travel)
    wave = math.sin(phase)
    direction = 1 if math.cos(phase) >= 0 else -1
    x = int(lane_left + ((wave + 1.0) / 2.0) * travel)
    frames = robot_frames(direction)
    frame = frames[int(tick * 6.0) % len(frames)]
    base_y = 1
    hop = int((math.sin(tick * 5.0) + 1.0) / 3.1)
    for row_index, (text, style_name) in enumerate(frame):
        canvas.draw_text(x, base_y + row_index - hop, text, style_name, max_width=robot_width)
    if canvas.width >= 92:
        runway = "upper lane active"
        runway_x = max(2, min(canvas.width - len(runway) - 2, lane_right + 2))
        canvas.draw_text(runway_x, 3, runway, "dim", max_width=canvas.width - runway_x - 1)


def draw_footer(canvas: Canvas, layout: Layout, snapshot: MetricsSnapshot) -> None:
    y = canvas.height - layout.footer_h
    canvas.draw_hline(1, y, canvas.width - 2, "─", "border")
    left = f"Status: {snapshot.status}"
    if snapshot.alerts:
        center = "Alerts: " + " | ".join(snapshot.alerts[:3])
    else:
        center = "Stable telemetry | neon channels synced"
    battery = snapshot.battery
    if battery.percent is not None:
        right = f"{'Charging' if battery.plugged else 'Battery'} {battery.percent:.0f}%"
    else:
        right = "Power AC / unavailable"
    canvas.draw_text(2, y + 1, truncate_plain(left, max(8, canvas.width // 4)), "footer")
    center_x = max(2, (canvas.width - len(center)) // 2)
    canvas.draw_text(
        center_x,
        y + 1,
        truncate_plain(center, max(10, canvas.width - 4)),
        "gold" if snapshot.alerts else "mint",
    )
    right_x = max(2, canvas.width - len(right) - 2)
    canvas.draw_text(right_x, y + 1, right, "footer")


def render_frame(
    width: int,
    height: int,
    config: AppConfig,
    static: StaticInfo,
    snapshot: MetricsSnapshot,
    tick: float,
) -> str:
    canvas = Canvas(width, height, config)
    layout = build_layout(width, height, config)
    if not layout.split and not layout.compact:
        draw_starfield(canvas, tick, density=80, top_skip=layout.header_h + 1)
    draw_header(canvas, layout, static, snapshot)
    draw_top_robot(canvas, tick, layout)
    if layout.split:
        draw_split_body(canvas, layout, snapshot)
    else:
        draw_single_body(canvas, layout, snapshot)
    draw_footer(canvas, layout, snapshot)
    if width < 72 or height < 22:
        warning = "Resize terminal for full Fitz Status grid"
        canvas.draw_text(
            max(1, (width - len(warning)) // 2),
            min(height - 2, 1),
            warning,
            "warn",
            max_width=max(0, width - 2),
        )
    return canvas.render()


# ------------------------------- Runtime ----------------------------------- #


class Application:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.running = True
        self.sampler = MetricSampler()
        time.sleep(0.08)
        self.snapshot = self._safe_sample()
        self.next_refresh = time.monotonic()
        self.frame_interval = 1.0 / max(2.0, config.frame_rate)

    def stop(self, *_args) -> None:
        self.running = False

    def _safe_sample(self) -> MetricsSnapshot:
        try:
            return self.sampler.sample()
        except Exception as exc:
            fallback = MetricsSnapshot(status="Telemetry Degraded")
            fallback.alerts = [truncate_plain(f"Metric failure: {exc.__class__.__name__}", 30)]
            return fallback

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        sys.stdout.write("\033[2J\033[?25l")
        sys.stdout.flush()
        try:
            while self.running:
                now = time.monotonic()
                if now >= self.next_refresh:
                    self.snapshot = self._safe_sample()
                    self.next_refresh = now + max(0.2, self.config.refresh_rate)
                width, height = shutil.get_terminal_size((128, 38))
                frame = render_frame(
                    width, height, self.config, self.sampler.static, self.snapshot, now
                )
                sys.stdout.write(frame)
                sys.stdout.flush()
                time.sleep(self.frame_interval)
        finally:
            sys.stdout.write("\033[0m\033[?25h\033[2J\033[H")
            sys.stdout.flush()


def main() -> int:
    if not sys.stdout.isatty():
        print("Fitz Status requires an interactive terminal.")
        return 1
    config = AppConfig()
    app = Application(config)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

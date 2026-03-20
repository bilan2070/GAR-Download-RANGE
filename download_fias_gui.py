#!/usr/bin/env python3
"""
FIAS GAR — Smart Range Downloader (GUI)
Скачивает только выбранные регионы через HTTP Range, не весь архив.
Python 3.8+, только stdlib. Запуск без консоли: переименовать в .pyw
"""
from __future__ import annotations
import dataclasses, logging, os, queue, re, shutil, struct, threading, time
import urllib.request, urllib.error, zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from tkinter import filedialog
from typing import Callable, Optional
import tkinter as tk

# ── Справочник регионов РФ ────────────────────────────────────────────────────
RF_REGIONS: dict[str, str] = {
    "01":"Республика Адыгея","02":"Республика Башкортостан",
    "03":"Республика Бурятия","04":"Республика Алтай",
    "05":"Республика Дагестан","06":"Республика Ингушетия",
    "07":"Кабардино-Балкарская Республика","08":"Республика Калмыкия",
    "09":"Карачаево-Черкесская Республика","10":"Республика Карелия",
    "11":"Республика Коми","12":"Республика Марий Эл",
    "13":"Республика Мордовия","14":"Республика Саха (Якутия)",
    "15":"Республика Северная Осетия — Алания","16":"Республика Татарстан",
    "17":"Республика Тыва","18":"Удмуртская Республика",
    "19":"Республика Хакасия","20":"Чеченская Республика (устар.)",
    "21":"Чувашская Республика","22":"Алтайский край",
    "23":"Краснодарский край","24":"Красноярский край",
    "25":"Приморский край","26":"Ставропольский край",
    "27":"Хабаровский край","28":"Амурская область",
    "29":"Архангельская область","30":"Астраханская область",
    "31":"Белгородская область","32":"Брянская область",
    "33":"Владимирская область","34":"Волгоградская область",
    "35":"Вологодская область","36":"Воронежская область",
    "37":"Ивановская область","38":"Иркутская область",
    "39":"Калининградская область","40":"Калужская область",
    "41":"Камчатский край","42":"Кемеровская область — Кузбасс",
    "43":"Кировская область","44":"Костромская область",
    "45":"Курганская область","46":"Курская область",
    "47":"Ленинградская область","48":"Липецкая область",
    "49":"Магаданская область","50":"Московская область",
    "51":"Мурманская область","52":"Нижегородская область",
    "53":"Новгородская область","54":"Новосибирская область",
    "55":"Омская область","56":"Оренбургская область",
    "57":"Орловская область","58":"Пензенская область",
    "59":"Пермский край","60":"Псковская область",
    "61":"Ростовская область","62":"Рязанская область",
    "63":"Самарская область","64":"Саратовская область",
    "65":"Сахалинская область","66":"Свердловская область",
    "67":"Смоленская область","68":"Тамбовская область",
    "69":"Тверская область","70":"Томская область",
    "71":"Тульская область","72":"Тюменская область",
    "73":"Ульяновская область","74":"Челябинская область",
    "75":"Забайкальский край","76":"Ярославская область",
    "77":"г. Москва","78":"г. Санкт-Петербург",
    "79":"Еврейская автономная область",
    "83":"Ненецкий автономный округ",
    "86":"Ханты-Мансийский АО — Югра",
    "87":"Чукотский автономный округ",
    "89":"Ямало-Ненецкий автономный округ",
    "91":"Республика Крым","92":"г. Севастополь",
    "95":"Чеченская Республика",
}

# ── Палитра ───────────────────────────────────────────────────────────────────
CLR = {
    "bg":"#0F1117","panel":"#1A1D27","border":"#2A2D3A",
    "accent":"#4F8EF7","accent2":"#7C5BF5",
    "success":"#2ECC71","warning":"#F39C12","error":"#E74C3C",
    "text":"#E8EAF0","text_dim":"#6B7280",
    "log_bg":"#080B10","log_info":"#A8B4C8","log_debug":"#4A5568",
    "log_error":"#FC8181","log_ok":"#68D391",
    "btn_hover":"#5B9BFF","progress_bg":"#1E2130",
    "check_on":"#4F8EF7","check_hover":"#252839",
}
FONT_MONO = ("Consolas", 9)
FONT_UI = ("Segoe UI", 10)
FONT_UI_B = ("Segoe UI", 10, "bold")
FONT_TITLE = ("Segoe UI", 13, "bold")
FONT_SMALL = ("Segoe UI", 8)

# ── Сеть: retry + валидация ───────────────────────────────────────────────────
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2.0  # секунды, умножается на попытку

_DATE_PATTERN = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")


def _retry(fn, retries: int = _MAX_RETRIES, label: str = "request"):
    """Вызывает fn() с повторами при сетевых ошибках."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except (urllib.error.URLError, TimeoutError, OSError) as ex:
            last_err = ex
            if attempt < retries:
                delay = _RETRY_BACKOFF * attempt
                logging.getLogger("fias").debug(
                    "Retry %d/%d для %s через %.0f сек: %s",
                    attempt, retries, label, delay, ex,
                )
                time.sleep(delay)
    raise RuntimeError(f"{label}: {last_err}") from last_err


def _rget(url: str, start: int, end: int, timeout: int = 120) -> bytes:
    """HTTP Range GET с валидацией 206 ответа."""
    def _do():
        req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            if code != 206:
                raise RuntimeError(
                    f"Ожидался 206 Partial Content, получен {code}. "
                    "Сервер не поддерживает Range-запросы."
                )
            return resp.read()
    return _retry(_do, label=f"Range {start}-{end}")


def _head(url: str) -> tuple[bool, int]:
    """HEAD-запрос: возвращает (поддержка_range, размер_файла)."""
    def _do():
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=30) as resp:
            accepts = resp.headers.get("Accept-Ranges", "").lower() == "bytes"
            length = int(resp.headers.get("Content-Length", 0))
            return accepts, length
    return _retry(_do, label="HEAD")


# ── Конфигурация ──────────────────────────────────────────────────────────────
def _calc_last_tuesday() -> str:
    """Вычисляет дату ближайшего прошедшего вторника (фиксируется один раз)."""
    t = datetime.now()
    d = (t.weekday() - 1) % 7
    return (t - timedelta(days=d)).strftime("%Y.%m.%d")


@dataclass(frozen=True)
class FiasConfig:
    base_dir: Path = field(default_factory=lambda: Path.home() / "gar" / "fias_downloads")
    url_base: str = "https://fias-file.nalog.ru/downloads"
    archive_name: str = "gar_xml.zip"
    weeks_to_keep: int = 2
    log_max_bytes: int = 5 * 1024 * 1024
    log_backup_count: int = 3
    region_folders: tuple[str, ...] = ("40",)
    # Дата фиксируется при создании конфига, не пересчитывается при каждом обращении
    last_tuesday: str = field(default_factory=_calc_last_tuesday)

    @property
    def log_file(self) -> Path:
        return self.base_dir / "script.log"

    @property
    def download_url(self) -> str:
        return f"{self.url_base}/{self.last_tuesday}/{self.archive_name}"

    @property
    def filtered_archive_name(self) -> str:
        """gar_xml_40.zip  /  gar_xml_40_77_78.zip  и т.д."""
        suffix = "_".join(sorted(self.region_folders))
        return f"gar_xml_{suffix}.zip"

    @property
    def filtered_archive_path(self) -> Path:
        return self.base_dir / self.filtered_archive_name

    @property
    def raw_archive_path(self) -> Path:
        return self.base_dir / self.last_tuesday / self.archive_name

    @property
    def download_dir(self) -> Path:
        return self.base_dir / self.last_tuesday


# ── Логгер ────────────────────────────────────────────────────────────────────
_LOGGER_NAME = "fias"


class QueueHandler(logging.Handler):
    def __init__(self, q):
        super().__init__()
        self._q = q

    def emit(self, r):
        self._q.put(r)


def setup_logger(config: FiasConfig, log_queue: queue.Queue) -> logging.Logger:
    """Возвращает логгер, переиспользуя существующий (без утечки хэндлеров)."""
    config.log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S")

    lg = logging.getLogger(_LOGGER_NAME)
    lg.setLevel(logging.DEBUG)

    # Закрываем и удаляем старые хэндлеры, чтобы не дублировать вывод
    for h in lg.handlers[:]:
        h.close()
        lg.removeHandler(h)

    fh = RotatingFileHandler(
        config.log_file, maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count, encoding="utf-8",
    )
    fh.setFormatter(fmt)

    qh = QueueHandler(log_queue)
    qh.setFormatter(fmt)

    lg.addHandler(fh)
    lg.addHandler(qh)
    return lg


# ── ZIP Range Downloader ──────────────────────────────────────────────────────
_LFH  = b"PK\x03\x04"   # Local File Header signature
_CDH  = b"PK\x01\x02"   # Central Directory Header signature
_EOCD = b"PK\x05\x06"   # End of Central Directory signature
_Z64L = b"PK\x06\x07"   # ZIP64 EOCD Locator signature

# ZIP64 limits
_MAX32 = 0xFFFFFFFF

# Сливать соседние записи если расстояние < 1 МБ
_MERGE_GAP = 1 * 1024 * 1024

# Запас на extra fields в Local File Header (бывает большой, особенно ZIP64)
_LFH_EXTRA_MARGIN = 4096


@dataclass
class ZipEntry:
    filename: str
    filename_bytes: bytes
    flags: int
    compress_method: int
    mod_time: int
    mod_date: int
    crc32: int
    compressed_size: int
    uncompressed_size: int
    local_header_offset: int
    version_made: int
    version_needed: int
    comment_bytes: bytes


def _find_eocd(data: bytes) -> int:
    for i in range(len(data) - 22, -1, -1):
        if data[i:i + 4] == _EOCD:
            return i
    raise RuntimeError("EOCD не найден — архив повреждён?")


def _parse_eocd(url: str, fsize: int) -> tuple[int, int, int]:
    tail_sz = min(65536 + 22, fsize)
    tail = _rget(url, fsize - tail_sz, fsize - 1)
    ep = _find_eocd(tail)

    # ZIP64 locator check
    z64p = ep - 20
    if z64p >= 0 and tail[z64p:z64p + 4] == _Z64L:
        z64off = struct.unpack_from("<Q", tail, z64p + 8)[0]
        z64 = _rget(url, z64off, z64off + 55)
        (_, _, _, _, _, _, entries, _, cd_size, cd_off) = struct.unpack_from(
            "<4sQHHIIQQQQ", z64,
        )
        return cd_off, cd_size, entries

    (_, _, _, _, entries, cd_size, cd_off, _) = struct.unpack_from("<4sHHHHIIH", tail, ep)
    return cd_off, cd_size, entries


def _parse_cd(data: bytes) -> list[ZipEntry]:
    out = []
    pos = 0
    while pos + 46 <= len(data):
        if data[pos:pos + 4] != _CDH:
            break
        (_, vm, vn, fl, cm, mt, md, crc, cs, us, fnl, exl, cml,
         _, _, _, lho) = struct.unpack_from("<4sHHHHHHIIIHHHHHII", data, pos)
        fn = data[pos + 46:pos + 46 + fnl]
        ex = data[pos + 46 + fnl:pos + 46 + fnl + exl]
        co = data[pos + 46 + fnl + exl:pos + 46 + fnl + exl + cml]

        # ZIP64 extra field
        if cs == _MAX32 or us == _MAX32 or lho == _MAX32:
            ep2 = 0
            while ep2 + 4 <= len(ex):
                eid, esz = struct.unpack_from("<HH", ex, ep2)
                if eid == 0x0001:
                    ef = ex[ep2 + 4:ep2 + 4 + esz]
                    fp = 0
                    if us == _MAX32 and fp + 8 <= len(ef):
                        us = struct.unpack_from("<Q", ef, fp)[0]; fp += 8
                    if cs == _MAX32 and fp + 8 <= len(ef):
                        cs = struct.unpack_from("<Q", ef, fp)[0]; fp += 8
                    if lho == _MAX32 and fp + 8 <= len(ef):
                        lho = struct.unpack_from("<Q", ef, fp)[0]
                    break
                ep2 += 4 + esz

        try:
            name = fn.decode("utf-8")
        except UnicodeDecodeError:
            name = fn.decode("cp437", errors="replace")

        out.append(ZipEntry(name, fn, fl, cm, mt, md, crc, cs, us, lho, vm, vn, co))
        pos += 46 + fnl + exl + cml
    return out


def _is_target(e: ZipEntry, regions: tuple[str, ...]) -> bool:
    """
    Проверяет, относится ли запись к выбранным регионам.
    Корневые файлы (без подпапки) включаются — это общие справочники ГАР.
    """
    if e.compressed_size == 0:
        return False
    p = Path(e.filename).parts
    return len(p) == 1 or p[0] in regions


def _batches(entries: list[ZipEntry]) -> list[list[ZipEntry]]:
    """Группирует записи в батчи для минимизации HTTP-запросов."""
    if not entries:
        return []
    se = sorted(entries, key=lambda e: e.local_header_offset)
    bs = [[se[0]]]
    for e in se[1:]:
        last = bs[-1][-1]
        # Оценка конца предыдущей записи с достаточным запасом на LFH extra fields
        end_estimate = (
            last.local_header_offset + 30
            + len(last.filename_bytes) + _LFH_EXTRA_MARGIN
            + last.compressed_size
        )
        if e.local_header_offset - end_estimate <= _MERGE_GAP:
            bs[-1].append(e)
        else:
            bs.append([e])
    return bs


class _ZipWriter:
    """
    Записывает ZIP из уже-сжатых блоков (без перепаковки).
    Поддерживает ZIP64 для файлов и архивов > 4 ГБ.
    Использует атомарную запись: пишет во временный файл, переименовывает при close().
    """
    def __init__(self, path: Path):
        self._final_path = path
        self._tmp_path = path.with_suffix(".tmp")
        self._f = open(self._tmp_path, "wb")
        self._cd: list[bytes] = []
        self._pos: int = 0

    def add(self, e: ZipEntry, raw: bytes):
        fn = e.filename_bytes
        raw_sz = len(raw)
        need_zip64 = (raw_sz > _MAX32 or e.uncompressed_size > _MAX32 or self._pos > _MAX32)

        if need_zip64:
            # ZIP64 extra field для Local File Header
            z64_extra = struct.pack("<HHQQQ", 0x0001, 24, e.uncompressed_size, raw_sz, self._pos)
            lh = struct.pack(
                "<4sHHHHHIIIHH", _LFH, 45,
                e.flags & ~8, e.compress_method, e.mod_time, e.mod_date,
                e.crc32, _MAX32, _MAX32, len(fn), len(z64_extra),
            )
            off = self._pos
            blk = lh + fn + z64_extra + raw
        else:
            lh = struct.pack(
                "<4sHHHHHIIIHH", _LFH, e.version_needed,
                e.flags & ~8, e.compress_method, e.mod_time, e.mod_date,
                e.crc32, raw_sz, e.uncompressed_size, len(fn), 0,
            )
            off = self._pos
            blk = lh + fn + raw

        self._f.write(blk)
        self._pos += len(blk)

        # Central Directory entry
        if need_zip64 or off > _MAX32:
            z64_cd = struct.pack("<HHQQQ", 0x0001, 24, e.uncompressed_size, raw_sz, off)
            cdh = struct.pack(
                "<4sHHHHHHIIIHHHHHII", _CDH, e.version_made, 45,
                e.flags & ~8, e.compress_method, e.mod_time, e.mod_date,
                e.crc32,
                _MAX32 if raw_sz > _MAX32 else raw_sz,
                _MAX32 if e.uncompressed_size > _MAX32 else e.uncompressed_size,
                len(fn), len(z64_cd), len(e.comment_bytes), 0, 0, 0,
                _MAX32 if off > _MAX32 else off,
            )
            self._cd.append(cdh + fn + z64_cd + e.comment_bytes)
        else:
            cdh = struct.pack(
                "<4sHHHHHHIIIHHHHHII", _CDH, e.version_made, e.version_needed,
                e.flags & ~8, e.compress_method, e.mod_time, e.mod_date,
                e.crc32, raw_sz, e.uncompressed_size,
                len(fn), 0, len(e.comment_bytes), 0, 0, 0, off,
            )
            self._cd.append(cdh + fn + e.comment_bytes)

    def close(self):
        cd_start = self._pos
        cd_data = b"".join(self._cd)
        self._f.write(cd_data)
        cd_end = self._pos + len(cd_data)

        n_entries = len(self._cd)
        need_zip64_eocd = (
            cd_start > _MAX32 or len(cd_data) > _MAX32 or n_entries > 0xFFFF
        )

        if need_zip64_eocd:
            # ZIP64 End of Central Directory Record
            z64_eocd_off = cd_end
            self._f.write(struct.pack(
                "<4sQHHIIQQQQ",
                b"PK\x06\x06", 44,  # size of remaining record
                45, 45, 0, 0,
                n_entries, n_entries,
                len(cd_data), cd_start,
            ))
            # ZIP64 End of Central Directory Locator
            self._f.write(struct.pack(
                "<4sIQI", _Z64L, 0, z64_eocd_off, 1,
            ))

        # Стандартный EOCD (с 0xFFFF / 0xFFFFFFFF если ZIP64)
        self._f.write(struct.pack(
            "<4sHHHHIIH", _EOCD, 0, 0,
            min(n_entries, 0xFFFF), min(n_entries, 0xFFFF),
            min(len(cd_data), _MAX32), min(cd_start, _MAX32),
            0,
        ))
        self._f.close()

        # Атомарная замена: tmp → final
        # На Windows нужно сначала удалить целевой файл
        if os.name == "nt" and self._final_path.exists():
            self._final_path.unlink()
        self._tmp_path.rename(self._final_path)

    def __enter__(self):
        return self

    def __exit__(self, et, *_):
        if et is None:
            self.close()
        else:
            self._f.close()
            # При ошибке удаляем битый временный файл
            self._tmp_path.unlink(missing_ok=True)


def _verify_zip(path: Path, logger: logging.Logger) -> None:
    """Быстрая проверка валидности собранного ZIP."""
    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad = zf.testzip()
            if bad:
                logger.warning("Повреждённый файл в архиве: %s", bad)
            else:
                logger.info("Проверка ZIP: OK (%d файлов)", len(zf.infolist()))
    except zipfile.BadZipFile as ex:
        logger.error("Собранный архив невалиден: %s", ex)
        raise RuntimeError(f"Собранный архив невалиден: {ex}") from ex


def smart_download(
    config: FiasConfig,
    logger: logging.Logger,
    on_progress: Callable[[float, float, float], None],
    on_status: Callable[[str], None],
    stop: threading.Event,
) -> None:
    url = config.download_url
    regions = config.region_folders
    config.base_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. HEAD ───────────────────────────────────────────────────────────────
    on_status("Проверка сервера...")
    logger.info("Архив: %s", url)
    try:
        range_ok, fsize = _head(url)
    except Exception as ex:
        raise RuntimeError(f"Сервер недоступен: {ex}") from ex

    total_mb = fsize / 1024 / 1024
    logger.info("Размер полного архива: %.1f МБ", total_mb)

    if not range_ok or fsize == 0:
        logger.warning("Range не поддерживается — полная загрузка (fallback)")
        _fallback(config, logger, on_progress, on_status, stop)
        return

    logger.info("Range-загрузка активна: скачиваю только нужные регионы")
    on_progress(1.0, 0, total_mb)
    if stop.is_set():
        raise InterruptedError()

    # ── 2. EOCD ───────────────────────────────────────────────────────────────
    on_status("Читаю структуру архива...")
    logger.info("Загружаю EOCD (последние 64 КБ)...")
    try:
        cd_off, cd_sz, n_entries = _parse_eocd(url, fsize)
    except Exception as ex:
        raise RuntimeError(f"Ошибка чтения ZIP: {ex}") from ex

    logger.debug("CD: offset=%d size=%.1f КБ entries=%d", cd_off, cd_sz / 1024, n_entries)
    on_progress(3.0, cd_sz / 1024 / 1024, total_mb)
    if stop.is_set():
        raise InterruptedError()

    # ── 3. Central Directory ──────────────────────────────────────────────────
    on_status("Загружаю оглавление архива...")
    logger.info("Загружаю Central Directory (%.1f КБ)...", cd_sz / 1024)
    try:
        cd_data = _rget(url, cd_off, cd_off + cd_sz - 1)
        entries = _parse_cd(cd_data)
    except Exception as ex:
        raise RuntimeError(f"Ошибка CD: {ex}") from ex

    logger.info("Всего файлов в архиве: %d", len(entries))
    on_progress(5.0, cd_sz / 1024 / 1024, total_mb)
    if stop.is_set():
        raise InterruptedError()

    # ── 4. Фильтрация ────────────────────────────────────────────────────────
    selected = [e for e in entries if _is_target(e, regions)]
    if not selected:
        raise RuntimeError(f"Нет файлов для регионов: {', '.join(sorted(regions))}")

    need_bytes = sum(e.compressed_size for e in selected)
    need_mb = need_bytes / 1024 / 1024
    saving = (1 - need_bytes / max(fsize, 1)) * 100
    rnames = ", ".join(f"{r} — {RF_REGIONS.get(r, '?')}" for r in sorted(regions))
    logger.info("Регионы: %s", rnames)
    logger.info("Файлов для загрузки: %d", len(selected))
    logger.info("Объём: %.1f МБ  (экономия %.0f%% от %.0f МБ)", need_mb, saving, total_mb)

    bs = _batches(selected)
    logger.info("HTTP Range-запросов: %d", len(bs))
    on_status(f"Загружаю {len(selected)} файлов ({len(bs)} запросов)...")
    if stop.is_set():
        raise InterruptedError()

    # ── 5. Скачиваем и собираем ZIP ──────────────────────────────────────────
    done = 0
    with _ZipWriter(config.filtered_archive_path) as zw:
        for bi, batch in enumerate(bs, 1):
            if stop.is_set():
                raise InterruptedError()

            f0 = batch[0]
            fl = batch[-1]
            rs = f0.local_header_offset
            re = (
                fl.local_header_offset + 30
                + len(fl.filename_bytes) + _LFH_EXTRA_MARGIN
                + fl.compressed_size
            )
            # Не выходим за размер файла
            re = min(re, fsize - 1)

            logger.debug(
                "Запрос %d/%d: %d файл(ов) %.1f–%.1f МБ",
                bi, len(bs), len(batch), rs / 1024 / 1024, re / 1024 / 1024,
            )
            try:
                blob = _rget(url, rs, re)
            except urllib.error.HTTPError as ex:
                raise RuntimeError(f"HTTP {ex.code} пакет {bi}") from ex

            for e in batch:
                if stop.is_set():
                    raise InterruptedError()

                rel = e.local_header_offset - rs
                if rel < 0 or rel + 4 > len(blob) or blob[rel:rel + 4] != _LFH:
                    raise RuntimeError(f"Плохая сигнатура LFH: {e.filename}")

                fnl2, exl2 = struct.unpack_from("<HH", blob, rel + 26)
                ds = rel + 30 + fnl2 + exl2
                de = ds + e.compressed_size
                if de > len(blob):
                    raise RuntimeError(f"Данные вышли за пределы блока: {e.filename}")

                zw.add(e, blob[ds:de])
                done += e.compressed_size
                pct = 5 + 90 * done / max(need_bytes, 1)
                on_progress(min(pct, 95.0), done / 1024 / 1024, need_mb)

    # ── 6. Проверка результата ────────────────────────────────────────────────
    on_status("Проверка архива...")
    on_progress(96.0, done / 1024 / 1024, need_mb)
    _verify_zip(config.filtered_archive_path, logger)

    sz = config.filtered_archive_path.stat().st_size / 1024 / 1024
    logger.info("Архив сохранён: %s (%.1f МБ)", config.filtered_archive_path, sz)
    on_progress(100.0, done / 1024 / 1024, need_mb)


def _fallback(config, logger, on_progress, on_status, stop):
    config.download_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Скачиваю полный архив...")
    on_status("Полная загрузка...")
    lpt = -1

    def hook(bn, bs2, ts):
        nonlocal lpt
        if stop.is_set():
            raise InterruptedError()
        if ts <= 0:
            return
        done = min(bn * bs2, ts)
        pt = int(done * 1000 / ts)
        if pt > lpt:
            lpt = pt
            on_progress(pt / 10, done / 1024 / 1024, ts / 1024 / 1024)

    try:
        urllib.request.urlretrieve(config.download_url, str(config.raw_archive_path), hook)
    except urllib.error.HTTPError as ex:
        raise RuntimeError(f"HTTP {ex.code}") from ex
    except urllib.error.URLError as ex:
        raise RuntimeError(f"Сеть: {ex.reason}") from ex

    if not config.raw_archive_path.exists() or config.raw_archive_path.stat().st_size == 0:
        raise RuntimeError("Скачанный файл пуст")

    on_status("Фильтрую архив...")
    tmp = config.download_dir / "filtered"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(config.raw_archive_path, "r") as src:
            ms = [
                m for m in src.infolist()
                if not m.is_dir()
                and _is_target(
                    ZipEntry(m.filename, m.filename.encode(), 0, 0, 0, 0,
                             m.CRC, m.compress_size, m.file_size, 0, 0, 0, b""),
                    config.region_folders,
                )
            ]
            for m in ms:
                src.extract(m, tmp)
        with zipfile.ZipFile(config.filtered_archive_path, "w", zipfile.ZIP_DEFLATED) as dst:
            for f in tmp.rglob("*"):
                if f.is_file():
                    dst.write(f, f.relative_to(tmp))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        config.raw_archive_path.unlink(missing_ok=True)
        try:
            config.download_dir.rmdir()
        except OSError:
            pass


def purge_old(config: FiasConfig, logger: logging.Logger) -> None:
    """
    Удаляет только файлы, созданные этим скриптом:
      - Отфильтрованные архивы вида gar_xml_*.zip в base_dir (старше N недель)
      - Подпапки с датами вида YYYY.MM.DD в base_dir (старше N недель)

    НЕ трогает никакие другие файлы и папки в base_dir.
    """
    cutoff = datetime.now().timestamp() - config.weeks_to_keep * 7 * 86400
    deleted = 0

    # 1. Удаляем отфильтрованные архивы вида gar_xml_*.zip старше N недель.
    #    Текущий архив (config.filtered_archive_name) не трогаем.
    for f in config.base_dir.glob("gar_xml_*.zip"):
        if f.name == config.filtered_archive_name:
            continue
        if f.is_file() and f.stat().st_mtime < cutoff:
            try:
                f.unlink()
                deleted += 1
                logger.debug("Удалён старый архив: %s", f.name)
            except OSError as ex:
                logger.warning("Не удалось удалить %s: %s", f, ex)

    # 2. Удаляем подпапки с датами вида YYYY.MM.DD — временные папки загрузки.
    for subdir in sorted(config.base_dir.iterdir(), reverse=True):
        if not subdir.is_dir():
            continue
        if not _DATE_PATTERN.match(subdir.name):
            continue
        if subdir.stat().st_mtime < cutoff:
            try:
                shutil.rmtree(subdir)
                deleted += 1
                logger.debug("Удалена старая папка: %s", subdir.name)
            except OSError as ex:
                logger.warning("Не удалось удалить папку %s: %s", subdir, ex)

    logger.info("Очистка: удалено %d объектов (старше %d нед.)", deleted, config.weeks_to_keep)


# ── Виджеты ───────────────────────────────────────────────────────────────────
class HoverButton(tk.Label):
    def __init__(self, parent, text, command, bg=None, bg_hover=None,
                 fg="white", disabled_bg=None, **kw):
        self._bg = bg or CLR["accent"]
        self._bgh = bg_hover or CLR["btn_hover"]
        self._bgd = disabled_bg or CLR["border"]
        self._cmd = command
        self._on = True
        super().__init__(
            parent, text=text, bg=self._bg, fg=fg, font=FONT_UI_B,
            cursor="hand2", padx=18, pady=8, relief="flat", **kw,
        )
        self.bind("<Enter>", lambda _: self._on and self.config(bg=self._bgh))
        self.bind("<Leave>", lambda _: self.config(bg=self._bg if self._on else self._bgd))
        self.bind("<Button-1>", lambda _: self._on and self._cmd())

    def set_enabled(self, v):
        self._on = v
        self.config(bg=self._bg if v else self._bgd, cursor="hand2" if v else "arrow")


class ProgressBar(tk.Canvas):
    def __init__(self, parent, **kw):
        super().__init__(
            parent, bg=CLR["progress_bg"], highlightthickness=1,
            highlightbackground=CLR["border"], height=28, **kw,
        )
        self._p = 0.0
        self.bind("<Configure>", lambda _: self._draw())

    def set(self, p):
        self._p = max(0.0, min(p, 100.0))
        self._draw()

    def _draw(self):
        self.delete("all")
        w, h = self.winfo_width(), self.winfo_height()
        if w < 2:
            return
        fw = int(w * self._p / 100)
        self.create_rectangle(0, 0, w, h, fill=CLR["progress_bg"], outline="")
        if fw > 0:
            for i in range(fw):
                t = i / max(fw - 1, 1)
                r = int(0x4F + (0x7C - 0x4F) * t)
                g = int(0x8E + (0x5B - 0x8E) * t)
                b = int(0xF7 + (0xF5 - 0xF7) * t)
                self.create_line(i, 2, i, h - 2, fill=f"#{r:02x}{g:02x}{b:02x}")
        self.create_text(
            w // 2, h // 2,
            text=f"{self._p:.1f}%" if self._p > 0 else "Ожидание...",
            fill="white", font=FONT_UI_B,
        )


class StatusBadge(tk.Frame):
    ST = {
        "idle": ("●", CLR["text_dim"], "Ожидание"),
        "running": ("●", CLR["accent"], "Загрузка..."),
        "processing": ("◈", CLR["warning"], "Обработка..."),
        "done": ("✔", CLR["success"], "Готово"),
        "error": ("✘", CLR["error"], "Ошибка"),
        "cancelled": ("■", CLR["warning"], "Остановлено"),
    }

    def __init__(self, parent):
        super().__init__(parent, bg=CLR["panel"])
        self._i = tk.Label(self, font=("Segoe UI", 11), bg=CLR["panel"])
        self._t = tk.Label(self, font=FONT_UI, bg=CLR["panel"])
        self._i.pack(side="left", padx=(0, 5))
        self._t.pack(side="left")
        self.set("idle")

    def set(self, state, extra=""):
        ic, co, lb = self.ST.get(state, self.ST["idle"])
        self._i.config(text=ic, fg=co)
        self._t.config(text=f"{lb}{(' — ' + extra) if extra else ''}", fg=co)

    def set_text(self, text):
        self._t.config(text=text)


class RegionPicker(tk.Frame):
    def __init__(self, parent, **kw):
        super().__init__(parent, bg=CLR["panel"], **kw)
        self._vars: dict[str, tk.BooleanVar] = {}
        self._rows: dict[str, tk.Frame] = {}
        self._ccs: dict[str, tk.Canvas] = {}
        self._build()

    def _build(self):
        h = tk.Frame(self, bg=CLR["panel"])
        h.pack(fill="x", pady=(0, 6))
        tk.Label(
            h, text="🗺  Регионы для извлечения", font=FONT_UI_B,
            fg=CLR["text"], bg=CLR["panel"],
        ).pack(side="left")
        self._cnt = tk.Label(
            h, text="выбрано: 0", font=FONT_SMALL,
            fg=CLR["accent"], bg=CLR["panel"],
        )
        self._cnt.pack(side="right")

        sf = tk.Frame(self, bg=CLR["panel"])
        sf.pack(fill="x", pady=(0, 6))
        self._sv = tk.StringVar()
        self._sv.trace_add("write", lambda *_: self._filter())
        tk.Entry(
            sf, textvariable=self._sv, font=FONT_UI, bg=CLR["log_bg"],
            fg=CLR["text"], insertbackground=CLR["accent"], relief="flat", bd=0,
        ).pack(side="left", fill="x", expand=True, ipady=5, padx=(0, 8))
        tk.Label(sf, text="🔍", font=FONT_UI, fg=CLR["text_dim"], bg=CLR["panel"]).pack(side="left")

        bf = tk.Frame(self, bg=CLR["panel"])
        bf.pack(fill="x", pady=(0, 6))
        for lbl, v in (("Выбрать все", True), ("Снять все", False)):
            HoverButton(
                bf, text=lbl, command=lambda x=v: self._all(x),
                bg=CLR["border"], bg_hover=CLR["accent"], fg=CLR["text_dim"],
            ).pack(side="left", padx=(0, 6))

        wr = tk.Frame(self, bg=CLR["border"], padx=1, pady=1)
        wr.pack(fill="both", expand=True)
        self._cv = tk.Canvas(wr, bg=CLR["panel"], highlightthickness=0, bd=0)
        sb = tk.Scrollbar(
            wr, orient="vertical", command=self._cv.yview,
            bg=CLR["border"], troughcolor=CLR["log_bg"],
        )
        self._cv.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._cv.pack(side="left", fill="both", expand=True)

        self._lf = tk.Frame(self._cv, bg=CLR["panel"])
        self._wi = self._cv.create_window((0, 0), window=self._lf, anchor="nw")

        self._lf.bind("<Configure>", lambda e: (
            self._cv.configure(scrollregion=self._cv.bbox("all")),
            self._cv.itemconfig(self._wi, width=self._cv.winfo_width()),
        ))
        self._cv.bind("<Configure>", lambda e: self._cv.itemconfig(self._wi, width=e.width))

        # Скролл мышью только когда курсор над canvas (а не глобально)
        self._cv.bind("<Enter>", lambda _: self._cv.bind_all(
            "<MouseWheel>", self._on_mousewheel))
        self._cv.bind("<Leave>", lambda _: self._cv.unbind_all("<MouseWheel>"))

        for code in sorted(RF_REGIONS):
            self._add_row(code)
        self._upd()

    def _on_mousewheel(self, event):
        self._cv.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _add_row(self, code):
        nm = RF_REGIONS[code]
        var = tk.BooleanVar(value=False)
        self._vars[code] = var

        row = tk.Frame(self._lf, bg=CLR["panel"], cursor="hand2")
        row.pack(fill="x", padx=4, pady=1)
        self._rows[code] = row

        cc = tk.Canvas(row, width=16, height=16, bd=0, highlightthickness=0, bg=CLR["panel"])
        cc.pack(side="left", padx=(6, 8), pady=4)
        self._ccs[code] = cc

        tk.Label(
            row, text=code, font=("Consolas", 9, "bold"), fg=CLR["accent"],
            bg=CLR["panel"], width=3, anchor="e",
        ).pack(side="left", padx=(0, 6))
        tk.Label(
            row, text=nm, font=FONT_UI, fg=CLR["text"],
            bg=CLR["panel"], anchor="w",
        ).pack(side="left", fill="x", expand=True)

        def tog(_c=code):
            self._vars[_c].set(not self._vars[_c].get())
            self._dc(_c)
            self._upd()

        for w in row.winfo_children() + [row]:
            w.bind("<Button-1>", lambda e, t=tog: t())
            w.bind("<Enter>", lambda e, r=row: r.config(bg=CLR["check_hover"]))
            w.bind("<Leave>", lambda e, r=row: r.config(bg=CLR["panel"]))
        self._dc(code)

    def _dc(self, code):
        cc = self._ccs[code]
        ck = self._vars[code].get()
        cc.delete("all")
        col = CLR["check_on"] if ck else CLR["border"]
        cc.create_rectangle(1, 1, 15, 15, outline=col, fill=CLR["panel"], width=2)
        if ck:
            cc.create_rectangle(4, 4, 12, 12, fill=CLR["check_on"], outline="")

    def _filter(self):
        q = self._sv.get().lower().strip()
        for c, r in self._rows.items():
            vis = not q or q in c or q in RF_REGIONS[c].lower()
            if vis:
                r.pack(fill="x", padx=4, pady=1)
            else:
                r.pack_forget()
        self._cv.yview_moveto(0)

    def _all(self, v):
        q = self._sv.get().lower().strip()
        for c in self._vars:
            if not q or q in c or q in RF_REGIONS[c].lower():
                self._vars[c].set(v)
                self._dc(c)
        self._upd()

    def _upd(self):
        n = sum(1 for v in self._vars.values() if v.get())
        self._cnt.config(text=f"выбрано: {n}", fg=CLR["accent"] if n else CLR["text_dim"])

    def get_selected(self) -> tuple[str, ...]:
        return tuple(sorted(k for k, v in self._vars.items() if v.get()))


# ── Главное окно ──────────────────────────────────────────────────────────────
class FiasApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self._cfg = FiasConfig()
        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S",
        )
        self._build_win()
        self._build_ui()
        self._poll()

    def _build_win(self):
        self.title("ФИАС ГАР — Range-загрузчик регионов")
        self.configure(bg=CLR["bg"])
        self.resizable(True, True)
        self.minsize(900, 640)
        self.update_idletasks()
        w, h = 1100, 760
        self.geometry(
            f"{w}x{h}+{(self.winfo_screenwidth() - w) // 2}"
            f"+{(self.winfo_screenheight() - h) // 2}"
        )

    def _build_ui(self):
        hdr = tk.Frame(self, bg=CLR["panel"])
        hdr.pack(fill="x", side="top")
        tk.Frame(hdr, bg=CLR["accent"], height=3).pack(fill="x")

        tr = tk.Frame(hdr, bg=CLR["panel"], padx=20, pady=14)
        tr.pack(fill="x")
        tk.Label(tr, text="ФИАС ГАР", font=FONT_TITLE, fg=CLR["accent"], bg=CLR["panel"]).pack(side="left")
        tk.Label(tr, text="  ФИАС ГАР Range-загрузчик", font=FONT_UI, fg=CLR["text_dim"], bg=CLR["panel"]).pack(side="left")
        tk.Label(tr, text="Provision LLC  |  2025", font=FONT_SMALL, fg=CLR["text_dim"], bg=CLR["panel"]).pack(side="right", padx=(0, 20))
        tk.Label(tr, text=f"Архив за:  {self._cfg.last_tuesday}", font=FONT_SMALL, fg=CLR["text_dim"], bg=CLR["panel"]).pack(side="right")

        tk.Frame(self, bg=CLR["border"], height=1).pack(fill="x", side="top")

        # Footer first (pack order matters)
        tk.Frame(self, bg=CLR["border"], height=1).pack(side="bottom", fill="x")
        ft = tk.Frame(self, bg=CLR["panel"], padx=20, pady=12)
        ft.pack(side="bottom", fill="x")
        self._cbtn = HoverButton(
            ft, text="⏹  Остановить", command=self._cancel,
            bg=CLR["border"], bg_hover="#5C1A1A", fg=CLR["error"],
        )
        self._cbtn.pack(side="right", padx=(8, 0))
        self._cbtn.set_enabled(False)
        self._sbtn = HoverButton(ft, text="▶  Начать загрузку", command=self._start)
        self._sbtn.pack(side="right")
        self._lbl = tk.Label(
            ft, text=f"Лог → {self._cfg.log_file}",
            font=FONT_SMALL, fg=CLR["text_dim"], bg=CLR["panel"],
        )
        self._lbl.pack(side="left")

        # Body
        body = tk.Frame(self, bg=CLR["bg"], padx=16, pady=14)
        body.pack(side="top", fill="both", expand=True)
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(0, weight=1)

        left = tk.Frame(body, bg=CLR["bg"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left.rowconfigure(2, weight=1)
        left.columnconfigure(0, weight=1)

        self._build_path(left)
        self._build_prog(left)
        self._build_log(left)

        right = tk.Frame(body, bg=CLR["panel"], padx=14, pady=12)
        right.grid(row=0, column=1, sticky="nsew")
        right.rowconfigure(0, weight=1)
        right.columnconfigure(0, weight=1)
        self._rp = RegionPicker(right)
        self._rp.pack(fill="both", expand=True)

    def _build_path(self, p):
        f = tk.Frame(p, bg=CLR["panel"], padx=14, pady=12)
        f.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        tk.Label(f, text="📁  Папка сохранения", font=FONT_UI_B, fg=CLR["text"], bg=CLR["panel"]).pack(anchor="w", pady=(0, 6))

        row = tk.Frame(f, bg=CLR["panel"])
        row.pack(fill="x")
        self._pv = tk.StringVar(value=str(self._cfg.base_dir))
        tk.Entry(
            row, textvariable=self._pv, font=FONT_MONO, bg=CLR["log_bg"],
            fg=CLR["text"], insertbackground=CLR["accent"], relief="flat", bd=0,
        ).pack(side="left", fill="x", expand=True, ipady=7, padx=(0, 10))
        HoverButton(row, text="Обзор...", command=self._browse, bg=CLR["border"], bg_hover=CLR["accent"]).pack(side="left")

    def _build_prog(self, p):
        f = tk.Frame(p, bg=CLR["panel"], padx=14, pady=12)
        f.grid(row=1, column=0, sticky="ew", pady=(0, 10))

        top = tk.Frame(f, bg=CLR["panel"])
        top.pack(fill="x", pady=(0, 8))
        tk.Label(top, text="⬇  Прогресс загрузки", font=FONT_UI_B, fg=CLR["text"], bg=CLR["panel"]).pack(side="left")
        self._sz = tk.Label(top, text="", font=FONT_SMALL, fg=CLR["text_dim"], bg=CLR["panel"])
        self._sz.pack(side="right")

        self._pb = ProgressBar(f)
        self._pb.pack(fill="x")

        sr = tk.Frame(f, bg=CLR["panel"])
        sr.pack(fill="x", pady=(8, 0))
        self._st = StatusBadge(sr)
        self._st.pack(side="left")

    def _build_log(self, p):
        f = tk.Frame(p, bg=CLR["panel"], padx=14, pady=12)
        f.grid(row=2, column=0, sticky="nsew")

        top = tk.Frame(f, bg=CLR["panel"])
        top.pack(fill="x", pady=(0, 6))
        tk.Label(top, text="📋  Журнал", font=FONT_UI_B, fg=CLR["text"], bg=CLR["panel"]).pack(side="left")
        HoverButton(
            top, text="Очистить", command=self._clear_log,
            bg=CLR["border"], bg_hover="#3A3D4A", fg=CLR["text_dim"],
        ).pack(side="right")

        wr = tk.Frame(f, bg=CLR["border"], padx=1, pady=1)
        wr.pack(fill="both", expand=True)
        self._log = tk.Text(
            wr, bg=CLR["log_bg"], fg=CLR["log_info"], font=FONT_MONO,
            relief="flat", state="disabled", wrap="word", padx=8, pady=6, cursor="arrow",
        )
        sb = tk.Scrollbar(wr, command=self._log.yview, bg=CLR["border"], troughcolor=CLR["log_bg"])
        self._log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._log.pack(side="left", fill="both", expand=True)

        for tag, col in (
            ("INFO", CLR["log_info"]), ("DEBUG", CLR["log_debug"]),
            ("ERROR", CLR["log_error"]), ("WARNING", CLR["warning"]),
            ("SUCCESS", CLR["log_ok"]),
        ):
            self._log.tag_config(tag, foreground=col)

    def _browse(self):
        c = filedialog.askdirectory(title="Папка для архива", initialdir=self._pv.get())
        if c:
            self._pv.set(c)

    def _clear_log(self):
        self._log.config(state="normal")
        self._log.delete("1.0", "end")
        self._log.config(state="disabled")

    def _append(self, text, tag):
        self._log.config(state="normal")
        self._log.insert("end", text + "\n", tag)
        self._log.see("end")
        self._log.config(state="disabled")

    def _poll(self):
        try:
            while True:
                rec = self._q.get_nowait()
                tag = rec.levelname if rec.levelname in ("DEBUG", "WARNING", "ERROR") else "INFO"
                msg = rec.getMessage()
                if any(w in msg for w in ("Готово", "✔", "сохранён", "создан")):
                    tag = "SUCCESS"
                self._append(self._fmt.format(rec), tag)
        except queue.Empty:
            pass
        self.after(50, self._poll)

    def _upd_prog(self, pct, done, total):
        self._pb.set(pct)
        self._sz.config(text=f"{done:.1f} / {total:.1f} МБ")

    def _start(self):
        raw = self._pv.get().strip()
        if not raw:
            self._st.set("error", "Не выбрана папка")
            return

        regions = self._rp.get_selected()
        if not regions:
            self._st.set("error", "Не выбран ни один регион")
            return

        d = Path(raw)
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception as ex:
            self._st.set("error", str(ex))
            return

        cfg = dataclasses.replace(self._cfg, base_dir=d, region_folders=regions)
        self._lbl.config(text=f"Лог → {cfg.log_file}")

        self._stop.clear()
        self._sbtn.set_enabled(False)
        self._cbtn.set_enabled(True)
        self._pb.set(0)
        self._sz.config(text="")
        self._st.set("running")

        self._worker = threading.Thread(target=self._run, args=(cfg,), daemon=True)
        self._worker.start()

    def _cancel(self):
        self._stop.set()
        self._cbtn.set_enabled(False)

    def _run(self, cfg):
        lg = setup_logger(cfg, self._q)
        lg.info("=" * 52)
        lg.info("Старт. Архив за: %s", cfg.last_tuesday)
        lg.info("Папка: %s", cfg.base_dir)
        lg.info("Регионов: %d", len(cfg.region_folders))
        lg.info("=" * 52)

        on_p = lambda p, d, t: self.after(0, self._upd_prog, p, d, t)
        on_s = lambda s: self.after(0, self._st.set_text, s)

        try:
            smart_download(cfg, lg, on_p, on_s, self._stop)
            if self._stop.is_set():
                lg.warning("Остановлено.")
                self.after(0, self._done_cancel)
                return
            purge_old(cfg, lg)
            lg.info("✔ Готово! Архив: %s", cfg.filtered_archive_path)
            self.after(0, self._done_ok)
        except InterruptedError:
            lg.warning("Остановлено.")
            self.after(0, self._done_cancel)
        except (RuntimeError, zipfile.BadZipFile, OSError) as ex:
            lg.error("Ошибка: %s", ex)
            self.after(0, self._done_err, str(ex))
        except Exception as ex:
            lg.exception("Непредвиденная ошибка: %s", ex)
            self.after(0, self._done_err, str(ex))

    def _done_ok(self):
        self._pb.set(100)
        self._st.set("done")
        self._sbtn.set_enabled(True)
        self._cbtn.set_enabled(False)

    def _done_err(self, m):
        self._st.set("error", m[:70])
        self._sbtn.set_enabled(True)
        self._cbtn.set_enabled(False)

    def _done_cancel(self):
        self._st.set("cancelled")
        self._sbtn.set_enabled(True)
        self._cbtn.set_enabled(False)


if __name__ == "__main__":
    FiasApp().mainloop()

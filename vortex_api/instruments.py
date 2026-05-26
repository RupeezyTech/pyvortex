"""
Instrument manager for the Vortex SDK.

Downloads the Vortex instruments master (``master.csv``) and exposes fast
lookups by ticker, ``(exchange, token)``, or ISIN. The master file is cached
both in memory and on disk so that ``master.csv`` is fetched at most once per
trading day, even across process restarts.

Typical use::

    from vortex_api import VortexAPI

    client = VortexAPI(...)

    inst = client.instruments.get_by_ticker("NSE:RELIANCE")
    print(inst.token, inst.exchange, inst.lot_size)

    # Reverse lookup
    inst = client.instruments.get_by_exchange_token("NSE_EQ", 2885)

    # All option contracts under NIFTY
    chain = client.instruments.all_by_underlying("NSE_FO", "NIFTY")
"""

from __future__ import annotations

import csv
import datetime
import gzip
import io
import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import requests

DEFAULT_MASTER_URL = "https://static.rupeezy.in/master.csv"
DEFAULT_REVALIDATE_AFTER_SECONDS = 15 * 60  # how long to trust disk before conditional GET
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
_IST_OFFSET = datetime.timedelta(hours=5, minutes=30)

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("pyvortex: ignoring invalid %s=%r (expected int)", name, raw)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("pyvortex: ignoring invalid %s=%r (expected float)", name, raw)
        return default


class InstrumentNotFound(Exception):
    """Raised when a lookup cannot find the requested instrument."""


@dataclass
class Instrument:
    """A single tradable instrument row from ``master.csv``.

    Fields mirror the columns produced by the Vortex master file. Missing or
    empty columns produce default values rather than raising.
    """

    ticker: str = ""           # e.g. "NSE:RELIANCE"
    token: int = 0
    exchange: str = ""         # e.g. "NSE_EQ", "NSE_FO"
    instrument_name: str = ""
    symbol: str = ""
    series: str = ""
    expiry_date: str = ""
    isin: str = ""
    option_type: str = ""
    strike_price: float = 0.0
    tick: float = 0.0
    lot_size: int = 0
    eligibility: bool = False
    name: str = ""
    last_trading_date: Optional[datetime.datetime] = None
    asm_gsm_stage: str = ""
    extras: Dict[str, str] = field(default_factory=dict)

    @property
    def exchange_token_key(self) -> str:
        """Composite key used internally for ``(exchange, token)`` lookups.

        Matches the Go package's ``TokenIdentifier`` format: first three
        characters of the exchange (NSE/BSE/MCX) joined with the token.
        """
        return _exchange_token_key(self.exchange, self.token)


def _exchange_token_key(exchange: str, token: int) -> str:
    short = (exchange or "")[:3]
    return f"{short}:{int(token)}"


def _now_ist() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).astimezone(
        datetime.timezone(_IST_OFFSET)
    )


def _default_cache_path() -> str:
    # Resolve where to cache master.csv. Honours (in order) PYVORTEX_CACHE_DIR,
    # XDG_CACHE_HOME, ~/.cache, then falls back to /tmp for locked-down
    # containers/serverless. First writable path wins.
    explicit = os.environ.get("PYVORTEX_CACHE_DIR")
    if explicit:
        return os.path.join(explicit, "master.csv")

    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg:
        return os.path.join(xdg, "pyvortex", "master.csv")

    home = os.environ.get("HOME")
    if home and home not in ("/", ""):
        expanded = os.path.expanduser("~")
        # ``expanduser`` returns ``~`` verbatim when no home can be resolved
        # (e.g. distroless container, no /etc/passwd entry). Guard against that.
        if expanded != "~":
            return os.path.join(expanded, ".cache", "pyvortex", "master.csv")

    return os.path.join(tempfile.gettempdir(), "pyvortex", "master.csv")


class InstrumentManager:
    """Thread-safe, lazily-loaded instruments cache.

    Args:
        master_url: Override the master CSV URL.
        cache_path: Where to persist the downloaded master file. Defaults to
            ``~/.cache/pyvortex/master.csv`` (with sensible fallbacks for
            containers and read-only filesystems).
        enable_disk_cache: If ``False``, every load re-downloads. Default ``True``.
            Mostly useful for tests; in production let the cache do its job.
        request_timeout: HTTP request timeout in seconds. Default 30.
        revalidate_after_seconds: How long to trust the disk cache without
            re-checking the server. Default 900 (15 minutes). Set to 0 to
            revalidate on every load.
    """

    def __init__(
        self,
        master_url: Optional[str] = None,
        cache_path: Optional[str] = None,
        enable_disk_cache: Optional[bool] = None,
        request_timeout: Optional[float] = None,
        revalidate_after_seconds: Optional[int] = None,
    ) -> None:
        self._master_url = (
            master_url
            or os.environ.get("PYVORTEX_MASTER_URL")
            or DEFAULT_MASTER_URL
        )
        self._cache_path = cache_path or _default_cache_path()
        self._meta_path = self._cache_path + ".meta.json"

        self._enable_disk_cache = True if enable_disk_cache is None else bool(enable_disk_cache)

        self._request_timeout = (
            request_timeout
            if request_timeout is not None
            else _env_float("PYVORTEX_REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT_SECONDS)
        )

        if revalidate_after_seconds is None:
            revalidate_after_seconds = _env_int(
                "PYVORTEX_REVALIDATE_SECONDS", DEFAULT_REVALIDATE_AFTER_SECONDS
            )
        self._revalidate_after_seconds = max(0, int(revalidate_after_seconds))

        self._lock = threading.RLock()
        self._by_ticker: Dict[str, Instrument] = {}
        self._by_exchange_token: Dict[str, Instrument] = {}
        self._by_isin: Dict[str, List[Instrument]] = {}
        self._last_loaded_at: Optional[datetime.datetime] = None
        self._loaded = False

        # Container-safety: if the configured cache path can't be written
        # (read-only rootfs is common on EKS / Cloud Run / Lambda), we transparently
        # fall back to a path under the OS temp dir. The fallback is computed
        # lazily and remembered so we don't spam logs.
        self._disk_disabled = False  # set True if even the /tmp fallback fails
        self._fallback_announced = False

    # ------------------------------------------------------------------ Public

    def load(self, force: bool = False) -> int:
        """Ensure instruments are loaded. Returns the instrument count.

        Freshness strategy (in order, cheapest first):

        1. In-memory cache: already populated this process → no I/O at all.
        2. Disk cache + revalidation cooldown: if we validated less than
           ``revalidate_after_seconds`` ago, trust the disk cache without
           touching the network.
        3. Conditional GET: send ``If-None-Match`` / ``If-Modified-Since`` from
           the cached metadata. A ``304`` response confirms the cache is
           current (a few hundred bytes on the wire). A ``200`` brings the new
           master and we re-index.
        4. Unconditional GET: only when there is no usable disk cache.

        Set ``force=True`` to bypass every cache and re-download.
        """
        with self._lock:
            if not force and self._loaded:
                return len(self._by_exchange_token)

            csv_text: Optional[str] = None
            meta = self._read_meta() if self._enable_disk_cache else None

            if not force and meta is not None and self._has_disk_cache():
                # (2) Recent revalidation → skip the network entirely.
                if self._within_revalidation_cooldown(meta):
                    csv_text = self._read_disk_cache()
                    if csv_text is not None:
                        logger.debug("Using disk cache (within cooldown): %s", self._cache_path)

                # (3) Otherwise issue a conditional GET.
                if csv_text is None:
                    try:
                        csv_text, new_meta = self._conditional_download(meta)
                    except requests.RequestException as exc:
                        # Network down but we have a usable disk cache — serve stale.
                        logger.warning(
                            "pyvortex: conditional refresh of instruments failed (%s). "
                            "Serving stale copy from disk cache (%s).",
                            exc, self._cache_path,
                        )
                        csv_text = self._read_disk_cache()
                    else:
                        if csv_text is None:
                            # 304 path — disk is still current.
                            csv_text = self._read_disk_cache()
                            self._touch_meta(meta, new_meta)
                        else:
                            # 200 path — write through.
                            self._write_disk_cache(csv_text)
                            self._write_meta(new_meta)

            if csv_text is None:
                # (4) No usable disk cache, or force=True.
                csv_text, new_meta = self._unconditional_download()
                if self._enable_disk_cache:
                    self._write_disk_cache(csv_text)
                    self._write_meta(new_meta)

            self._parse_and_index(csv_text)
            self._last_loaded_at = _now_ist()
            self._loaded = True
            return len(self._by_exchange_token)

    def refresh(self) -> int:
        """Force a network refresh, bypassing both caches."""
        return self.load(force=True)

    def count(self) -> int:
        with self._lock:
            return len(self._by_exchange_token)

    @property
    def is_loaded(self) -> bool:
        """``True`` once instruments have been loaded into memory.

        Useful in hot paths (e.g. the WebSocket tick parser) where a blocking
        lazy-load would be unacceptable: check this first and skip enrichment
        until a background ``.load()`` has populated the cache.
        """
        return self._loaded

    def get_ticker_or_none(self, exchange: str, token: int) -> Optional[str]:
        """Non-blocking variant of ``token_to_ticker``.

        Returns the ticker if the manager is already loaded *and* the lookup
        succeeds. Returns ``None`` if the manager hasn't loaded yet, or if the
        instrument isn't known. Never raises, never triggers a network fetch.
        Designed for the WebSocket tick parser.
        """
        if not self._loaded:
            return None
        key = _exchange_token_key(exchange, token)
        with self._lock:
            inst = self._by_exchange_token.get(key)
        return inst.ticker if inst is not None else None

    # ------------------------------------------------------------- Lookups

    def get_by_ticker(self, ticker: str) -> Instrument:
        self._ensure_loaded()
        with self._lock:
            try:
                return self._by_ticker[ticker]
            except KeyError:
                raise InstrumentNotFound(f"ticker not found: {ticker!r}")

    def get_by_exchange_token(self, exchange: str, token: int) -> Instrument:
        self._ensure_loaded()
        key = _exchange_token_key(exchange, token)
        with self._lock:
            try:
                return self._by_exchange_token[key]
            except KeyError:
                raise InstrumentNotFound(
                    f"instrument not found for exchange={exchange!r}, token={token}"
                )

    def get_by_isin(self, isin: str) -> List[Instrument]:
        self._ensure_loaded()
        with self._lock:
            insts = self._by_isin.get(isin)
            if not insts:
                raise InstrumentNotFound(f"ISIN not found: {isin!r}")
            return list(insts)

    def ticker_to_token(self, ticker: str) -> "tuple[str, int]":
        inst = self.get_by_ticker(ticker)
        return inst.exchange, inst.token

    def token_to_ticker(self, exchange: str, token: int) -> str:
        return self.get_by_exchange_token(exchange, token).ticker

    def all_by_underlying(self, exchange: str, underlying: str) -> List[Instrument]:
        """Return all instruments on ``exchange`` whose ``symbol`` matches
        ``underlying`` (typically used to fetch an option chain)."""
        self._ensure_loaded()
        with self._lock:
            return [
                i for i in self._by_exchange_token.values()
                if i.exchange == exchange and i.symbol == underlying
            ]

    def filter(self, predicate: Callable[[Instrument], bool]) -> List[Instrument]:
        self._ensure_loaded()
        with self._lock:
            return [i for i in self._by_exchange_token.values() if predicate(i)]

    # --------------------------------------------------------- Internals

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()

    # ---- cache I/O ----

    def _has_disk_cache(self) -> bool:
        return os.path.exists(self._cache_path)

    def _read_disk_cache(self) -> Optional[str]:
        try:
            with open(self._cache_path, "r", encoding="utf-8") as fh:
                return fh.read()
        except OSError as exc:
            logger.warning("Failed to read instruments cache %s: %s", self._cache_path, exc)
            return None

    def _write_disk_cache(self, csv_text: str) -> None:
        try:
            new_path = self._atomic_write_with_fallback(self._cache_path, csv_text)
        except OSError:
            # The fallback helper already emitted a one-time warning explaining
            # what happened. Disk cache is now disabled; in-memory still works.
            return
        if new_path != self._cache_path:
            # Disk cache moved — repoint everything so future loads use it.
            self._cache_path = new_path
            self._meta_path = new_path + ".meta.json"

    def _read_meta(self) -> Optional[Dict[str, str]]:
        if not os.path.exists(self._meta_path):
            return None
        try:
            with open(self._meta_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except (OSError, ValueError) as exc:
            logger.warning("Failed to read instruments meta %s: %s", self._meta_path, exc)
        return None

    def _write_meta(self, meta: Dict[str, str]) -> None:
        # Meta is small; on failure we silently skip it. Without meta the next
        # process will lose conditional-GET capability but still works correctly.
        try:
            self._atomic_write_with_fallback(self._meta_path, json.dumps(meta))
        except OSError as exc:
            logger.debug("Skipping instruments meta write (%s): %s", self._meta_path, exc)

    def _touch_meta(self, old: Dict[str, str], new: Dict[str, str]) -> None:
        """Merge ``new`` validation timestamp / headers into ``old`` and persist."""
        merged = dict(old)
        merged.update(new)
        self._write_meta(merged)

    def _atomic_write(self, path: str, text: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".master-", suffix=".tmp", dir=os.path.dirname(path)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _atomic_write_with_fallback(self, path: str, text: str) -> str:
        """Atomic write with auto-fallback to ``/tmp`` on read-only filesystems.

        Returns the path that was actually written. If even the fallback fails,
        the disk cache is disabled for this manager and a one-time warning is
        emitted; subsequent loads still work from memory.
        """
        try:
            self._atomic_write(path, text)
            return path
        except OSError as primary_exc:
            # Try /tmp as a fallback — this covers EKS readOnlyRootFilesystem,
            # Cloud Run, Lambda, and similar locked-down container setups.
            fallback_dir = os.path.join(tempfile.gettempdir(), "pyvortex")
            fallback_path = os.path.join(
                fallback_dir, os.path.basename(path)
            )
            if fallback_path == path:
                # Already trying /tmp — give up gracefully.
                self._announce_disk_disabled(path, primary_exc)
                raise
            try:
                self._atomic_write(fallback_path, text)
            except OSError as fallback_exc:
                self._announce_disk_disabled(path, fallback_exc)
                raise
            self._announce_fallback(path, fallback_path, primary_exc)
            return fallback_path

    def _announce_fallback(
        self, original: str, fallback: str, exc: OSError
    ) -> None:
        if self._fallback_announced:
            return
        self._fallback_announced = True
        logger.warning(
            "pyvortex: cannot write instruments cache to %s (%s). "
            "Falling back to %s for the rest of this process. "
            "To silence this warning in containers, set "
            "PYVORTEX_CACHE_DIR=/tmp/pyvortex (or any writable path).",
            original, exc, fallback,
        )

    def _announce_disk_disabled(self, path: str, exc: OSError) -> None:
        if self._disk_disabled:
            return
        self._disk_disabled = True
        logger.warning(
            "pyvortex: disk cache disabled (could not write to %s or its fallback: %s). "
            "Instruments will be loaded into memory only — expect a fresh download "
            "from %s every time this process starts. To fix, mount a writable "
            "volume and set PYVORTEX_CACHE_DIR to point at it.",
            path, exc, self._master_url,
        )

    # ---- freshness ----

    def _within_revalidation_cooldown(self, meta: Dict[str, str]) -> bool:
        if self._revalidate_after_seconds <= 0:
            return False
        fetched_at = meta.get("fetched_at")
        if not fetched_at:
            return False
        try:
            ts = datetime.datetime.fromisoformat(fetched_at)
        except ValueError:
            return False
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        age = datetime.datetime.now(datetime.timezone.utc) - ts
        return age.total_seconds() < self._revalidate_after_seconds

    # ---- network ----

    def _conditional_download(
        self, meta: Dict[str, str]
    ) -> "tuple[Optional[str], Dict[str, str]]":
        """Issue a conditional GET. Returns ``(csv_text_or_None, new_meta)``.

        ``csv_text`` is ``None`` iff the server returned ``304 Not Modified``
        (in which case ``new_meta`` only carries an updated ``fetched_at``).
        """
        headers = {"Accept-Encoding": "gzip"}
        if meta.get("etag"):
            headers["If-None-Match"] = meta["etag"]
        if meta.get("last_modified"):
            headers["If-Modified-Since"] = meta["last_modified"]

        logger.debug("Conditional GET master.csv (etag=%s)", meta.get("etag"))
        resp = requests.get(self._master_url, headers=headers, timeout=self._request_timeout)

        if resp.status_code == 304:
            logger.debug("master.csv unchanged (304); reusing disk cache")
            return None, {"fetched_at": _utc_now_iso()}

        resp.raise_for_status()
        return _decode_body(resp.content), _meta_from_response(resp)

    def _unconditional_download(self) -> "tuple[str, Dict[str, str]]":
        logger.debug("Downloading master.csv from %s", self._master_url)
        resp = requests.get(
            self._master_url,
            headers={"Accept-Encoding": "gzip"},
            timeout=self._request_timeout,
        )
        resp.raise_for_status()
        return _decode_body(resp.content), _meta_from_response(resp)

    def _parse_and_index(self, csv_text: str) -> None:
        reader = csv.DictReader(io.StringIO(csv_text))
        by_ticker: Dict[str, Instrument] = {}
        by_exchange_token: Dict[str, Instrument] = {}
        by_isin: Dict[str, List[Instrument]] = {}

        for row in reader:
            inst = _row_to_instrument(row)
            if inst.ticker:
                by_ticker[inst.ticker] = inst
            if inst.exchange and inst.token:
                by_exchange_token[inst.exchange_token_key] = inst
            if inst.isin:
                by_isin.setdefault(inst.isin, []).append(inst)

        # Atomic swap
        self._by_ticker = by_ticker
        self._by_exchange_token = by_exchange_token
        self._by_isin = by_isin
        logger.debug("Indexed %d instruments", len(by_exchange_token))


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _decode_body(content: bytes) -> str:
    """Decode a response body that may be gzipped at the application layer.

    ``requests`` already strips ``Content-Encoding: gzip`` transparently, but
    the static asset is sometimes served as ``application/octet-stream`` with
    the gzip magic bytes intact — handle both.
    """
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return content.decode("utf-8")


def _meta_from_response(resp: requests.Response) -> Dict[str, str]:
    meta: Dict[str, str] = {"fetched_at": _utc_now_iso()}
    etag = resp.headers.get("ETag")
    if etag:
        meta["etag"] = etag
    last_modified = resp.headers.get("Last-Modified")
    if last_modified:
        meta["last_modified"] = last_modified
    return meta


def _safe_int(v: str) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_float(v: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _row_to_instrument(row: Dict[str, str]) -> Instrument:
    last_trading_date: Optional[datetime.datetime] = None
    raw_ltd = row.get("last_trading_date") or ""
    if raw_ltd:
        try:
            # Master file stores epoch seconds shifted by IST offset
            # (see Go parser: `time.Unix(int64(t)-19800, 0)`).
            secs = float(raw_ltd) - 19800
            last_trading_date = datetime.datetime.fromtimestamp(
                secs, tz=datetime.timezone(_IST_OFFSET)
            )
        except (TypeError, ValueError):
            last_trading_date = None

    known = {
        "ticker", "symbol", "token", "exchange", "instrument_name", "series",
        "expiry_date", "isin_code", "option_type", "strike_price", "tick",
        "lot_size", "eligibility", "security_desc", "last_trading_date",
        "asm_gsm_stage",
    }
    extras = {k: v for k, v in row.items() if k not in known and v}

    return Instrument(
        ticker=row.get("ticker", "") or "",
        token=_safe_int(row.get("token", "")),
        exchange=row.get("exchange", "") or "",
        instrument_name=row.get("instrument_name", "") or "",
        symbol=row.get("symbol", "") or "",
        series=row.get("series", "") or "",
        expiry_date=row.get("expiry_date", "") or "",
        isin=row.get("isin_code", "") or "",
        option_type=row.get("option_type", "") or "",
        strike_price=_safe_float(row.get("strike_price", "")),
        tick=_safe_float(row.get("tick", "")),
        lot_size=_safe_int(row.get("lot_size", "")),
        eligibility=_safe_int(row.get("eligibility", "")) > 0,
        name=row.get("security_desc", "") or "",
        last_trading_date=last_trading_date,
        asm_gsm_stage=row.get("asm_gsm_stage", "") or "",
        extras=extras,
    )

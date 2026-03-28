"""Avoid UnicodeEncodeError on Windows (cp1252) when printing/logging UTF-8 text."""

from __future__ import annotations

import builtins
import io
import sys

_print_patched = False


def _try_set_console_utf8() -> None:
    """Set Windows console to UTF-8 code page (65001) when possible."""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)
    except Exception:
        pass


def _install_safe_print() -> None:
    """
    If a print still hits cp1252 (stale bytecode, odd IDE pipes, etc.),
    re-encode string args with errors=replace and retry once.
    """
    global _print_patched
    if sys.platform != "win32" or _print_patched:
        return
    _print_patched = True
    _orig = builtins.print

    def _safe_print(*args, **kwargs):
        try:
            return _orig(*args, **kwargs)
        except UnicodeEncodeError:
            file = kwargs.get("file")
            if file is None:
                file = sys.stdout
            enc = getattr(file, "encoding", None) or "utf-8"
            repaired: list[object] = []
            for a in args:
                if isinstance(a, str):
                    try:
                        repaired.append(
                            a.encode(enc, errors="replace").decode(
                                enc, errors="replace"
                            )
                        )
                    except Exception:
                        repaired.append(
                            a.encode("ascii", errors="replace").decode("ascii")
                        )
                else:
                    repaired.append(a)
            try:
                return _orig(*repaired, **kwargs)
            except UnicodeEncodeError:
                return _orig(
                    *(
                        str(x).encode("ascii", errors="replace").decode("ascii")
                        if isinstance(x, str)
                        else x
                        for x in repaired
                    ),
                    **kwargs,
                )

    builtins.print = _safe_print  # type: ignore[assignment]


def configure_stdio_utf8() -> None:
    """
    Force UTF-8 (or lossy replace) for stdout/stderr on Windows.

    Order: console code page -> safe print -> stream reconfigure / buffer wrap.
    """
    if sys.platform != "win32":
        return

    _try_set_console_utf8()
    _install_safe_print()

    def _fix(name: str) -> None:
        stream = getattr(sys, name, None)
        if stream is None:
            return
        enc = (getattr(stream, "encoding", None) or "").lower().replace("-", "_")
        if enc in ("utf8", "utf_8"):
            return
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
            enc2 = (getattr(stream, "encoding", None) or "").lower().replace("-", "_")
            if enc2 in ("utf8", "utf_8"):
                return
        except (AttributeError, OSError, ValueError, TypeError):
            pass
        try:
            buf = stream.buffer
        except AttributeError:
            return
        setattr(
            sys,
            name,
            io.TextIOWrapper(
                buf,
                encoding="utf-8",
                errors="replace",
                line_buffering=(name == "stdout"),
                write_through=True,
            ),
        )

    _fix("stdout")
    _fix("stderr")

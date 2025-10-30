"""Runtime glue for Rust-backed compute kernels.

The :func:`rust` decorator keeps the public Python API intact while delegating
heavy computation to an extension module when it is present.  Callers provide
an ordinary Python implementation; at runtime the wrapper attempts to import
the requested Rust module and, on success, forwards the call using optional
argument/result adapters.  When the module is missing the original Python
implementation is executed instead, so existing graphs continue to work.
"""

from __future__ import annotations

import importlib
import os
import sys
from functools import update_wrapper
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Tuple

RustArgAdapter = Callable[..., Tuple[Tuple[Any, ...], dict]]
RustResultAdapter = Callable[[Any, Tuple[Any, ...], dict], Any]


def _default_arg_adapter(*args: Any, **kwargs: Any) -> Tuple[Tuple[Any, ...], dict]:
    return args, kwargs


def _default_result_adapter(result: Any, _args: Tuple[Any, ...], _kwargs: dict) -> Any:
    return result


class RustFunction:
    """Callable proxy that lazily resolves the Rust implementation."""

    def __init__(
        self,
        python_impl: Callable[..., Any],
        module_name: str,
        func_name: Optional[str] = None,
        *,
        paths: Optional[Iterable[Callable[[], Optional[Path]] | str | Path]] = None,
        arg_adapter: Optional[RustArgAdapter] = None,
        result_adapter: Optional[RustResultAdapter] = None,
        eager: bool = False,
    ) -> None:
        self.python_impl = python_impl
        self.module_name = module_name
        self.func_name = func_name or python_impl.__name__
        self._paths = tuple(paths or ())
        self._arg_adapter = arg_adapter or _default_arg_adapter
        self._result_adapter = result_adapter or _default_result_adapter
        self._callable: Optional[Callable[..., Any]] = None
        self._import_error: Optional[Exception] = None

        update_wrapper(self, python_impl)
        setattr(self, "__wrapped__", python_impl)
        setattr(self, "__rust__", True)
        setattr(self, "__python_impl__", python_impl)

        if eager:
            self._load()

    # -- Public API -----------------------------------------------------

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        rust_callable = self._load()
        if rust_callable is None:
            return self.python_impl(*args, **kwargs)

        rust_args, rust_kwargs = self._prepare_args(*args, **kwargs)
        result = rust_callable(*rust_args, **rust_kwargs)
        return self._process_result(result, args, kwargs)

    # -- Internal helpers ----------------------------------------------

    def _prepare_args(self, *args: Any, **kwargs: Any) -> Tuple[Tuple[Any, ...], dict]:
        adapted = self._arg_adapter(*args, **kwargs)
        if not isinstance(adapted, tuple) or len(adapted) != 2:
            raise TypeError(
                "Rust arg adapter must return a tuple of (args, kwargs), "
                f"got {type(adapted).__name__}"
            )
        rust_args, rust_kwargs = adapted
        if not isinstance(rust_args, tuple):
            raise TypeError(
                "Rust arg adapter must return positional arguments as a tuple, "
                f"got {type(rust_args).__name__}"
            )
        if not isinstance(rust_kwargs, dict):
            raise TypeError(
                "Rust arg adapter must return keyword arguments as a dict, "
                f"got {type(rust_kwargs).__name__}"
            )
        return rust_args, rust_kwargs

    def _process_result(
        self, result: Any, args: Tuple[Any, ...], kwargs: dict
    ) -> Any:
        return self._result_adapter(result, args, kwargs)

    def _candidate_paths(self) -> Iterable[Path]:
        env_paths = os.getenv("SUCURI_RUST_PATH", "")
        for raw in self._paths + tuple(filter(None, env_paths.split(os.pathsep))):
            if callable(raw):
                resolved = raw()
            else:
                resolved = raw
            if not resolved:
                continue
            path = Path(resolved)
            if path.exists() and str(path) not in sys.path:
                yield path

    def _load(self) -> Optional[Callable[..., Any]]:
        if self._callable is not None:
            return self._callable
        if self._import_error is not None:
            return None

        try:
            module = importlib.import_module(self.module_name)
        except ModuleNotFoundError:
            for path in self._candidate_paths():
                sys.path.insert(0, str(path))
            try:
                module = importlib.import_module(self.module_name)
            except ModuleNotFoundError as exc:
                self._import_error = exc
                return None
        try:
            rust_callable = getattr(module, self.func_name)
        except AttributeError as exc:
            self._import_error = exc
            return None

        self._callable = rust_callable
        return rust_callable


def rust(
    module: str,
    func: Optional[str] = None,
    *,
    paths: Optional[Iterable[Callable[[], Optional[Path]] | str | Path]] = None,
    arg_adapter: Optional[RustArgAdapter] = None,
    result_adapter: Optional[RustResultAdapter] = None,
    eager: bool = False,
) -> Callable[[Callable[..., Any]], RustFunction]:
    """Decorate a Python implementation with an optional Rust fast path.

    Parameters
    ----------
    module:
        Name of the Python extension module produced by the Rust crate.
    func:
        Symbol to call inside the module.  Defaults to the wrapped function's
        name.
    paths:
        Optional iterable of search paths (or callables returning paths) that
        should be appended to ``sys.path`` before importing the module.
    arg_adapter:
        Callable that receives the original arguments and must return a pair
        ``(args_tuple, kwargs_dict)`` describing how to invoke the Rust symbol.
        When omitted the original arguments are forwarded unchanged.
    result_adapter:
        Callable invoked with ``(result, original_args, original_kwargs)`` to
        translate the Rust return value back into the Python graph contract.
    eager:
        When ``True`` the module is imported during decoration rather than at
        the first call, raising immediately if it cannot be loaded.
    """

    def decorator(python_impl: Callable[..., Any]) -> RustFunction:
        return RustFunction(
            python_impl,
            module,
            func_name=func,
            paths=paths,
            arg_adapter=arg_adapter,
            result_adapter=result_adapter,
            eager=eager,
        )

    return decorator

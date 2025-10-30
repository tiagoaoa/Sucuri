"""Runtime glue for Rust-backed compute kernels with plugin support."""

from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from functools import update_wrapper
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Protocol, Tuple

RustArgAdapter = Callable[..., Tuple[Tuple[Any, ...], dict]]
RustResultAdapter = Callable[[Any, Tuple[Any, ...], dict], Any]


def _default_arg_adapter(*args: Any, **kwargs: Any) -> Tuple[Tuple[Any, ...], dict]:
    return args, kwargs


def _default_result_adapter(result: Any, _args: Tuple[Any, ...], _kwargs: dict) -> Any:
    return result


@dataclass(frozen=True)
class RustConfig:
    module: str
    func: Optional[str] = None
    paths: Tuple[Callable[[], Optional[Path]] | str | Path, ...] = ()
    arg_adapter: Optional[RustArgAdapter] = None
    result_adapter: Optional[RustResultAdapter] = None
    eager: bool = False


class RustPlugin(Protocol):
    """Plugins provide configuration for the :func:`rust` decorator."""

    def configure(self, python_impl: Callable[..., Any]) -> Optional[RustConfig]:
        ...


_PLUGINS: List[RustPlugin] = []
_PLUGINS_LOADED = False


def register_plugin(plugin: RustPlugin) -> None:
    """Register a plugin that can supply :class:`RustConfig` objects."""

    _PLUGINS.append(plugin)


def _load_builtin_plugins() -> None:
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return
    _PLUGINS_LOADED = True
    try:
        import pyDF.plugins  # noqa: F401  (triggers plugin registration)
    except ModuleNotFoundError:
        # Optional package; continue without built-ins.
        _PLUGINS_LOADED = True


class RustFunction:
    """Callable proxy that lazily resolves the Rust implementation."""

    def __init__(
        self,
        python_impl: Callable[..., Any],
        config: RustConfig,
    ) -> None:
        self.python_impl = python_impl
        self.module_name = config.module
        self.func_name = config.func or python_impl.__name__
        self._paths = tuple(config.paths or ())
        self._arg_adapter = config.arg_adapter or _default_arg_adapter
        self._result_adapter = config.result_adapter or _default_result_adapter
        self._callable: Optional[Callable[..., Any]] = None
        self._import_error: Optional[Exception] = None

        update_wrapper(self, python_impl)
        setattr(self, "__wrapped__", python_impl)
        setattr(self, "__rust__", True)
        setattr(self, "__python_impl__", python_impl)

        if config.eager:
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


def _config_from_kwargs(
    python_impl: Callable[..., Any],
    module: Optional[str],
    func: Optional[str],
    paths: Optional[Iterable[Callable[[], Optional[Path]] | str | Path]],
    arg_adapter: Optional[RustArgAdapter],
    result_adapter: Optional[RustResultAdapter],
    eager: bool,
) -> Optional[RustConfig]:
    if module is None:
        return None
    return RustConfig(
        module=module,
        func=func,
        paths=tuple(paths or ()),
        arg_adapter=arg_adapter,
        result_adapter=result_adapter,
        eager=eager,
    )


def _config_from_plugins(python_impl: Callable[..., Any]) -> Optional[RustConfig]:
    _load_builtin_plugins()
    for plugin in _PLUGINS:
        config = plugin.configure(python_impl)
        if config is not None:
            return config
    return None


def _apply_rust(
    python_impl: Callable[..., Any],
    module: Optional[str],
    func: Optional[str],
    paths: Optional[Iterable[Callable[[], Optional[Path]] | str | Path]],
    arg_adapter: Optional[RustArgAdapter],
    result_adapter: Optional[RustResultAdapter],
    eager: bool,
) -> RustFunction:
    config = _config_from_kwargs(python_impl, module, func, paths, arg_adapter, result_adapter, eager)
    if config is None:
        config = _config_from_plugins(python_impl)
    if config is None:
        raise RuntimeError(
            f"No Rust configuration found for {python_impl.__module__}.{python_impl.__name__}. "
            "Provide parameters to @rust or register a plugin."
        )
    return RustFunction(python_impl, config)


def rust(
    _func: Optional[Callable[..., Any]] = None,
    *,
    module: Optional[str] = None,
    func: Optional[str] = None,
    paths: Optional[Iterable[Callable[[], Optional[Path]] | str | Path]] = None,
    arg_adapter: Optional[RustArgAdapter] = None,
    result_adapter: Optional[RustResultAdapter] = None,
    eager: bool = False,
) -> Callable[[Callable[..., Any]], RustFunction] | RustFunction:
    """Decorate a Python implementation with an optional Rust fast path.

    The decorator may be invoked either with configuration arguments or without
    parameters to request automatic configuration via the plugin registry.
    """

    if _func is not None and callable(_func):
        if any(param is not None for param in (module, func, paths, arg_adapter, result_adapter)) or eager:
            raise TypeError("Cannot pass configuration when using @rust without parentheses.")
        return _apply_rust(_func, None, None, None, None, None, False)

    def decorator(python_impl: Callable[..., Any]) -> RustFunction:
        return _apply_rust(python_impl, module, func, paths, arg_adapter, result_adapter, eager)

    return decorator

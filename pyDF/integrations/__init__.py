"""Integration helpers for optional accelerators."""

from .rust import RustConfig, RustPlugin, register_plugin, rust

__all__ = ["rust", "register_plugin", "RustConfig", "RustPlugin"]

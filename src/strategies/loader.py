from __future__ import annotations

import hashlib
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from .core import Strategy, StrategyMetadata
from .simple_yaml import parse_simple_yaml_file

DEFAULT_BUILTIN_STRATEGY_PATH = Path(__file__).resolve().parent / "builtin" / "atr.py"


@dataclass(frozen=True)
class StrategyModuleConfig:
    path: Path
    class_name: str | None
    factory_name: str | None
    reload_on_change: bool


@dataclass(frozen=True)
class StrategyDefinition:
    config_path: Path
    metadata: StrategyMetadata
    config: dict[str, Any]
    module: StrategyModuleConfig


def resolve_strategy_module_path(
    raw_path: str | None,
    *,
    config_path: Path,
    repo_root: Path,
) -> Path:
    if raw_path is None or raw_path.strip() == "":
        return DEFAULT_BUILTIN_STRATEGY_PATH.resolve()

    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    repo_candidate = (repo_root / candidate).resolve()
    if repo_candidate.exists():
        return repo_candidate

    return (config_path.parent / candidate).resolve()


def load_strategy_definition(config_path: Path, *, repo_root: Path) -> StrategyDefinition:
    parsed = parse_simple_yaml_file(config_path)
    module_config = parsed.get("module", {}) if isinstance(parsed.get("module"), dict) else {}
    module_path = resolve_strategy_module_path(
        str(module_config.get("path")) if module_config.get("path") is not None else None,
        config_path=config_path,
        repo_root=repo_root,
    )

    metadata = StrategyMetadata(
        strategy_name=str(parsed.get("strategy_name", config_path.stem)),
        strategy_version=str(parsed.get("strategy_version", "unknown")),
        description=str(parsed.get("description", "")),
        config_path=str(config_path.resolve()),
        module_path=str(module_path),
    )
    return StrategyDefinition(
        config_path=config_path.resolve(),
        metadata=metadata,
        config=parsed,
        module=StrategyModuleConfig(
            path=module_path,
            class_name=(
                str(module_config.get("class_name")) if module_config.get("class_name") is not None else None
            ),
            factory_name=(
                str(module_config.get("factory_name"))
                if module_config.get("factory_name") is not None
                else "build_strategy"
            ),
            reload_on_change=bool(module_config.get("reload_on_change", True)),
        ),
    )


def load_strategy_module(module_path: Path) -> ModuleType:
    if not module_path.exists():
        raise FileNotFoundError(f"Strategy module not found: {module_path}")

    digest = hashlib.sha1(str(module_path.resolve()).encode("utf-8")).hexdigest()[:12]
    module_name = f"donkey_strategy_{digest}_{module_path.stat().st_mtime_ns}"
    module = ModuleType(module_name)
    module.__file__ = str(module_path)
    module.__package__ = ""
    sys.modules[module_name] = module
    source = module_path.read_text(encoding="utf-8")
    code = compile(source, str(module_path), "exec")
    exec(code, module.__dict__)
    return module


def resolve_strategy_target(module: ModuleType, module_config: StrategyModuleConfig) -> Any:
    candidate_names: list[str] = []
    if module_config.class_name is not None:
        candidate_names.append(module_config.class_name)
    if module_config.factory_name is not None:
        candidate_names.append(module_config.factory_name)
    candidate_names.extend(["build_strategy", "Strategy", "ATRStrategy"])

    seen: set[str] = set()
    for name in candidate_names:
        if name in seen:
            continue
        seen.add(name)
        if hasattr(module, name):
            return getattr(module, name)

    raise AttributeError(
        f"Strategy module {module.__file__} does not expose any supported strategy entrypoint."
    )


def invoke_strategy_target(target: Any, definition: StrategyDefinition) -> Strategy:
    signature = inspect.signature(target)
    positional_parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )

    if has_varargs or len(positional_parameters) >= 2:
        instance = target(definition.config, definition.metadata)
    elif len(positional_parameters) == 1:
        instance = target(definition.config)
    else:
        instance = target()

    if not hasattr(instance, "generate_signals"):
        raise TypeError(
            f"Strategy target {target!r} from {definition.metadata.module_path} "
            "must return an object with generate_signals()."
        )

    return instance


class ReloadableStrategyLoader:
    def __init__(self, config_path: Path, *, repo_root: Path | None = None) -> None:
        self.config_path = config_path.resolve()
        self.repo_root = (repo_root or Path.cwd()).resolve()
        self._definition: StrategyDefinition | None = None
        self._strategy: Strategy | None = None
        self._config_mtime_ns: int | None = None
        self._module_mtime_ns: int | None = None

    @property
    def definition(self) -> StrategyDefinition:
        if self._definition is None:
            self.refresh()
        assert self._definition is not None
        return self._definition

    def refresh(self) -> bool:
        definition = load_strategy_definition(self.config_path, repo_root=self.repo_root)
        config_mtime_ns = definition.config_path.stat().st_mtime_ns
        module_mtime_ns = definition.module.path.stat().st_mtime_ns

        needs_reload = self._strategy is None or self._definition is None
        if not needs_reload:
            assert self._definition is not None
            needs_reload = (
                config_mtime_ns != self._config_mtime_ns
                or definition.module.path != self._definition.module.path
                or definition.module.class_name != self._definition.module.class_name
                or definition.module.factory_name != self._definition.module.factory_name
            )
            if definition.module.reload_on_change:
                needs_reload = needs_reload or module_mtime_ns != self._module_mtime_ns

        if not needs_reload:
            return False

        module = load_strategy_module(definition.module.path)
        target = resolve_strategy_target(module, definition.module)
        self._strategy = invoke_strategy_target(target, definition)
        self._definition = definition
        self._config_mtime_ns = config_mtime_ns
        self._module_mtime_ns = module_mtime_ns
        return True

    def get_strategy(self) -> Strategy:
        self.refresh()
        assert self._strategy is not None
        return self._strategy

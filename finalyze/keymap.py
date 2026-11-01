"""The web app's keyboard shortcuts: the registry of actions and its clashes.

The registry is `keymap.json`, next to this module, which the web app reads too
(`web/src/lib/keymap.ts`). Both check a keymap with the same rules, so the
server refuses what the Settings page refuses. Keep the two in step.
"""

import functools
import json
from pathlib import Path
from typing import Any, NamedTuple

KEYMAP_FILE = Path(__file__).with_name("keymap.json")
GO_PREFIX = "go.prefix"
DIRECTIONS = ("left", "down", "up", "right")


class BoundKey(NamedTuple):
    key: str
    action: str
    fixed: bool


@functools.cache
def registry() -> dict[str, Any]:
    return json.loads(KEYMAP_FILE.read_text())


def actions() -> list[dict[str, Any]]:
    """Every action, with its scope."""
    return [
        {**action, "scope": scope["id"]}
        for scope in registry()["scopes"]
        for action in scope["actions"]
    ]


@functools.cache
def actions_by_id() -> dict[str, dict[str, Any]]:
    return {action["id"]: action for action in actions()}


def is_reserved(key: str) -> bool:
    """Whether a key can't be bound: a reserved one, alone or with Shift."""
    return key.removeprefix("Shift+") in registry()["reserved"]


def _shifted(key: str) -> str:
    return registry()["shifted"].get(key, key.upper())


def default_keys(action: dict[str, Any], directions: dict[str, str]) -> list[str]:
    """An action's default keys, which can be changed, under the direction keys."""
    direction = action.get("direction")
    if not direction:
        return action.get("keys", [])
    key = directions[direction]
    return [_shifted(key) if action.get("shift") else key]


def own_keys(
    directions: dict[str, str], overrides: dict[str, list[str]], action_id: str
) -> list[str]:
    """An action's keys that can be changed, as the keymap sets them. An
    invalid override (a go key that isn't one key) falls back to the default."""
    action = actions_by_id().get(action_id)
    if not action:
        return []
    override = overrides.get(action_id)
    if override is not None and not (action.get("single") and len(override) != 1):
        return override
    return default_keys(action, directions)


def keys_of(
    directions: dict[str, str], overrides: dict[str, list[str]], action_id: str
) -> list[str]:
    """The keys an action answers to: its own (a page's after the go key, e.g.
    "g t"), then its fixed ones."""
    action = actions_by_id().get(action_id)
    if not action:
        return []
    own = own_keys(directions, overrides, action_id)
    if action.get("navigation"):
        go_key = own_keys(directions, overrides, GO_PREFIX)[0]
        own = [f"{go_key} {letter}" for letter in own]
    return [*own, *action.get("fixed", [])]


def _describe(action_id: str) -> str:
    action = actions_by_id().get(action_id)
    return action["description"] if action else action_id


def clashes(
    directions: dict[str, str], overrides: dict[str, list[str]]
) -> list[tuple[str, tuple[str, str], str]]:
    """Keys that do two actions active together, as (key, the two actions'
    IDs, the context): the same key, or a key that starts another's sequence
    (e.g. "g" and "g t"). Fixed keys only clash with keys that can be changed:
    the ones they share are by design (e.g. Escape)."""
    found: dict[str, tuple[str, tuple[str, str], str]] = {}
    for context in registry()["contexts"]:
        keys: list[BoundKey] = []
        for action in actions():
            if action["scope"] not in context["scopes"]:
                continue
            all_keys = keys_of(directions, overrides, action["id"])
            own_count = len(all_keys) - len(action.get("fixed", []))
            keys.extend(
                BoundKey(key, action["id"], fixed=index >= own_count)
                for index, key in enumerate(all_keys)
            )
        for index, first in enumerate(keys):
            for second in keys[index + 1 :]:
                if first.action == second.action or (first.fixed and second.fixed):
                    continue
                shorter, longer = sorted(
                    (first, second), key=lambda bound: len(bound.key)
                )
                # The go key starting its pages' sequences is how they work
                starts = (
                    longer.key.startswith(f"{shorter.key} ")
                    and shorter.action != GO_PREFIX
                )
                if shorter.key != longer.key and not starts:
                    continue
                pair = tuple(sorted((first.action, second.action)))
                found.setdefault(
                    "\n".join([shorter.key, *pair]),
                    (shorter.key, pair, context["name"]),
                )
    return list(found.values())


def problems(directions: dict[str, str], overrides: dict[str, list[str]]) -> list[str]:
    """What keeps a keymap from being saved, as sentences: reserved keys, a key
    for two directions, and clashes."""
    reserved = [
        f"{key} can't be bound ({_describe(action_id)}): it keeps its own meaning"
        for action_id, keys in overrides.items()
        for key in keys
        if is_reserved(key)
    ]
    reserved += [
        f"{directions[direction]} can't be a direction ({direction}): "
        "it keeps its own meaning"
        for direction in DIRECTIONS
        if is_reserved(directions[direction])
    ]
    shared = [
        f"{directions[first]} is both {first} and {second}"
        for index, first in enumerate(DIRECTIONS)
        for second in DIRECTIONS[index + 1 :]
        if directions[first] == directions[second]
    ]
    clash_texts = [
        f'{key} is both "{_describe(pair[0])}" and "{_describe(pair[1])}" ({context})'
        for key, pair, context in clashes(directions, overrides)
    ]
    return [*reserved, *shared, *clash_texts]

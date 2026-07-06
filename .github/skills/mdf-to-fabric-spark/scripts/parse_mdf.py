#!/usr/bin/env python3
"""
parse_mdf.py — Tokenize an Azure Data Factory / Synapse Mapping Data Flow Script (DFS)
into individual transformation blocks and print them in topological (source -> sink) order.

This is a helper for the `mdf-to-fabric-spark` skill. It does NOT generate Spark code; it
gives the agent a reliable, ordered view of the transformation graph so translation is
accurate for large / deeply-nested flows.

Usage:
    python parse_mdf.py <path-to-dfs.txt>
    python parse_mdf.py <path-to-dfs.txt> --json
    cat dfs.txt | python parse_mdf.py -

Output (human-readable, default):
    - the parsed parameters block
    - each transformation: output name, type, inputs, and raw body
    - the topological order and detected branch points / multi-input joins

Notes:
    - Pure standard library, no dependencies.
    - Handles nested parentheses/brackets, single/double quoted strings, and the
      `name@(out1, out2)` multi-output split syntax.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict


@dataclass
class Transformation:
    output: str                      # primary output stream name
    type: str                        # transformation keyword (source, filter, join, ...)
    inputs: list[str] = field(default_factory=list)   # input stream names
    body: str = ""                   # raw text inside the (...) of the transformation
    extra_outputs: list[str] = field(default_factory=list)  # named split outputs
    raw: str = ""                    # the full original statement


def _strip_parameters(text: str) -> tuple[str, str]:
    """Split off a leading parameters{...} block. Returns (params_text, rest)."""
    i = 0
    n = len(text)
    while i < n and text[i].isspace():
        i += 1
    if not text.startswith("parameters", i):
        return "", text
    brace = text.find("{", i)
    if brace == -1:
        return "", text
    depth = 0
    j = brace
    while j < n:
        c = text[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                params = text[i : j + 1]
                return params, text[j + 1 :]
        j += 1
    return "", text  # unbalanced; treat as no params


def _iter_statements(text: str):
    """Yield raw statements split on the `~> name` terminator at paren-depth 0.

    A statement runs from the current position through the output stream name that
    follows a top-level `~>`. The output name may carry a `@(a, b, ...)` suffix.
    """
    depth = 0
    quote = None
    i = 0
    n = len(text)
    start = 0
    while i < n:
        c = text[i]
        if quote:
            if c == quote:
                quote = None
            i += 1
            continue
        if c in "'\"":
            quote = c
            i += 1
            continue
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif c == "~" and depth == 0 and i + 1 < n and text[i + 1] == ">":
            # consume "~>", then read the output name (and optional @(...) group)
            j = i + 2
            while j < n and text[j].isspace():
                j += 1
            name_start = j
            while j < n and (text[j].isalnum() or text[j] in "_"):
                j += 1
            # optional @(...) split outputs
            if j < n and text[j] == "@":
                k = j + 1
                if k < n and text[k] == "(":
                    pd = 0
                    while k < n:
                        if text[k] == "(":
                            pd += 1
                        elif text[k] == ")":
                            pd -= 1
                            if pd == 0:
                                k += 1
                                break
                        k += 1
                    j = k
                else:
                    while j < n and (text[j].isalnum() or text[j] in "_"):
                        j += 1
            stmt = text[start:j]
            yield stmt.strip()
            i = j
            start = i
            continue
        i += 1


_KNOWN_TYPES = {
    "source", "sink", "select", "filter", "derive", "aggregate", "join", "lookup",
    "exists", "union", "split", "window", "rank", "keyGenerate", "surrogateKey",
    "sort", "alterRow", "pivot", "unpivot", "flatten", "parse", "cast", "stringify",
    "flowlet", "assert", "externalCall", "sink cache", "call",
}


def _parse_statement(stmt: str) -> Transformation | None:
    """Parse one statement of the form:  [in1, in2 ] type( body ) ~> out[@(a,b)]"""
    if "~>" not in stmt:
        return None
    left, _, out_part = stmt.rpartition("~>")
    out_part = out_part.strip()

    extra_outputs: list[str] = []
    output = out_part
    if "@" in out_part:
        output, _, grp = out_part.partition("@")
        output = output.strip()
        grp = grp.strip()
        if grp.startswith("(") and grp.endswith(")"):
            extra_outputs = [s.strip() for s in grp[1:-1].split(",") if s.strip()]

    left = left.strip()
    # Find the transformation keyword: the last identifier immediately before a '('
    paren = left.find("(")
    if paren == -1:
        return None
    # walk left from paren to get the type token
    k = paren - 1
    while k >= 0 and left[k].isspace():
        k -= 1
    end = k + 1
    while k >= 0 and (left[k].isalnum() or left[k] in "_"):
        k -= 1
    ttype = left[k + 1 : end]
    inputs_part = left[: k + 1].strip()

    inputs: list[str] = []
    if inputs_part:
        inputs = [s.strip() for s in inputs_part.split(",") if s.strip()]

    # body = balanced content of the first (...) after the type
    depth = 0
    quote = None
    body_start = paren + 1
    j = paren
    body_end = len(left)
    while j < len(left):
        c = left[j]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
            if depth == 0:
                body_end = j
                break
        j += 1
    body = left[body_start:body_end].strip()

    return Transformation(
        output=output,
        type=ttype,
        inputs=inputs,
        body=body,
        extra_outputs=extra_outputs,
        raw=stmt.strip(),
    )


def topo_sort(transforms: list[Transformation]) -> list[Transformation]:
    """Return transforms ordered so every input is produced before it is consumed.

    Stream names include split outputs (name@out). Falls back to original order for
    any cycle / unresolved input (and reports it).
    """
    produced: dict[str, Transformation] = {}
    for t in transforms:
        produced[t.output] = t
        for eo in t.extra_outputs:
            produced[f"{t.output}@{eo}"] = t

    visited: set[str] = set()
    ordered: list[Transformation] = []
    temp: set[str] = set()

    def deps(t: Transformation) -> list[Transformation]:
        result = []
        for inp in t.inputs:
            key = inp
            if key in produced:
                result.append(produced[key])
            elif "@" in inp and inp.split("@")[0] in produced:
                result.append(produced[inp.split("@")[0]])
        return result

    def visit(t: Transformation):
        if t.output in visited:
            return
        if t.output in temp:
            return  # cycle; skip to avoid infinite loop
        temp.add(t.output)
        for d in deps(t):
            visit(d)
        temp.discard(t.output)
        visited.add(t.output)
        ordered.append(t)

    for t in transforms:
        visit(t)
    return ordered


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Parse an MDF Data Flow Script into ordered transformations.")
    ap.add_argument("path", help="Path to the DFS file, or '-' for stdin")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    args = ap.parse_args(argv)

    if args.path == "-":
        text = sys.stdin.read()
    else:
        with open(args.path, "r", encoding="utf-8") as f:
            text = f.read()

    params, rest = _strip_parameters(text)
    transforms: list[Transformation] = []
    for stmt in _iter_statements(rest):
        if not stmt:
            continue
        t = _parse_statement(stmt)
        if t:
            transforms.append(t)

    ordered = topo_sort(transforms)

    # branch detection: streams consumed by more than one transformation
    consumption: dict[str, int] = {}
    for t in transforms:
        for inp in t.inputs:
            base = inp.split("@")[0]
            consumption[base] = consumption.get(base, 0) + 1
    branches = [name for name, count in consumption.items() if count > 1]
    multi_input = [t.output for t in transforms if len(t.inputs) > 1]

    if args.json:
        print(json.dumps(
            {
                "parameters": params,
                "transformations": [asdict(t) for t in ordered],
                "branch_points": branches,
                "multi_input_transforms": multi_input,
            },
            indent=2,
        ))
        return 0

    print("=" * 70)
    print("PARAMETERS")
    print("=" * 70)
    print(params.strip() or "(none)")
    print()
    print("=" * 70)
    print(f"TRANSFORMATIONS (topological order) — {len(ordered)} total")
    print("=" * 70)
    for idx, t in enumerate(ordered, 1):
        ins = ", ".join(t.inputs) if t.inputs else "(source)"
        outs = t.output + (f" @({', '.join(t.extra_outputs)})" if t.extra_outputs else "")
        print(f"[{idx:>2}] {outs}")
        print(f"     type   : {t.type}")
        print(f"     inputs : {ins}")
        body_preview = (t.body[:200] + " ...") if len(t.body) > 200 else t.body
        print(f"     body   : {body_preview}")
        print()

    print("=" * 70)
    print("GRAPH NOTES")
    print("=" * 70)
    print(f"branch points (stream used by >1 transform): {branches or '(none)'}")
    print(f"multi-input transforms (join/union/lookup/exists): {multi_input or '(none)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

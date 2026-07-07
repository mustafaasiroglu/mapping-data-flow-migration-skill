#!/usr/bin/env python3
"""Export all mapping data flow (DFS) scripts from an Azure Data Factory.

Systematizes the manual steps used interactively:
  1. az datafactory data-flow list   -> enumerate data flows in the factory
  2. az datafactory data-flow show    -> fetch each definition
  3. reconstruct the DFS from `scriptLines` and write it to adf_exports/<name>.txt

Auth is handled by the Azure CLI (`az login`); this script shells out to `az`.

Usage:
    python export_mdf_scripts.py \
        --subscription 3d984a4e-a846-4574-bf60-aa3c826379bc \
        --resource-group rg-finops \
        --factory finops-hub-engine-alpvss5h2nybg

All arguments are optional and fall back to the defaults below.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# --- Defaults (override on the command line) ---
DEFAULT_SUBSCRIPTION = "3d984a4e-a846-4574-bf60-aa3c826379bc"
DEFAULT_RESOURCE_GROUP = "rg-finops"
DEFAULT_FACTORY = "finops-hub-engine-alpvss5h2nybg"
DEFAULT_OUTPUT_DIR = "adf_exports"

# `az` is a batch script on Windows, so it must run through the shell there.
_AZ_SHELL = sys.platform.startswith("win")


def run_az(args: list[str]) -> object:
    """Run an `az ... -o json` command and return the parsed JSON payload."""
    cmd = ["az", *args, "-o", "json"]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            shell=_AZ_SHELL,
        )
    except FileNotFoundError:
        sys.exit("ERROR: Azure CLI ('az') not found on PATH. Install it and run 'az login'.")
    except subprocess.CalledProcessError as exc:
        sys.exit(f"ERROR: command failed: {' '.join(cmd)}\n{exc.stderr.strip()}")

    out = result.stdout.strip()
    if not out:
        return None
    return json.loads(out)


def sanitize_filename(name: str) -> str:
    """Make a data flow name safe to use as a file name."""
    invalid = '<>:"/\\|?*'
    cleaned = "".join("_" if ch in invalid else ch for ch in name).strip()
    return cleaned or "unnamed"


def extract_script(properties: dict) -> str:
    """Reconstruct the DFS text from a data flow's properties."""
    script = properties.get("script")
    if script:
        return script
    lines = properties.get("scriptLines")
    if lines:
        return "\n".join(lines)
    return ""


def export_data_flows(subscription: str, resource_group: str, factory: str, output_dir: Path) -> int:
    """List every data flow in the factory and write each DFS to a .txt file.

    Returns the number of scripts exported.
    """
    print(f"Setting active subscription: {subscription}")
    run_az(["account", "set", "--subscription", subscription])

    print(f"Listing data flows in factory '{factory}' (rg: {resource_group})...")
    data_flows = run_az(
        [
            "datafactory",
            "data-flow",
            "list",
            "--factory-name",
            factory,
            "--resource-group",
            resource_group,
        ]
    ) or []

    if not data_flows:
        print("No data flows found. If the factory is Git-integrated, make sure changes are published.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    exported = 0
    skipped_non_mdf = 0

    for flow in data_flows:
        name = flow.get("name", "")
        properties = flow.get("properties") or {}

        # `list` may not include the full script; fetch the definition explicitly.
        detail = run_az(
            [
                "datafactory",
                "data-flow",
                "show",
                "--name",
                name,
                "--factory-name",
                factory,
                "--resource-group",
                resource_group,
            ]
        )
        detail_props = (detail or {}).get("properties") or properties

        flow_type = detail_props.get("type")
        if flow_type and flow_type != "MappingDataFlow":
            print(f"  - {name}: skipping (type={flow_type}, not a mapping data flow)")
            skipped_non_mdf += 1
            continue

        script = extract_script(detail_props)
        if not script:
            print(f"  - {name}: no script content, skipping")
            continue

        target = output_dir / f"{sanitize_filename(name)}.txt"
        target.write_text(script + "\n", encoding="utf-8")
        print(f"  - {name} -> {target}")
        exported += 1

    print(
        f"\nDone. Exported {exported} script(s) to '{output_dir}'."
        + (f" Skipped {skipped_non_mdf} non-mapping flow(s)." if skipped_non_mdf else "")
    )
    return exported


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export all mapping data flow (DFS) scripts from an Azure Data Factory."
    )
    parser.add_argument("--subscription", default=DEFAULT_SUBSCRIPTION, help="Azure subscription ID")
    parser.add_argument("--resource-group", default=DEFAULT_RESOURCE_GROUP, help="Resource group name")
    parser.add_argument("--factory", default=DEFAULT_FACTORY, help="Data Factory name")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Folder to write the .txt scripts into (default: adf_exports)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    export_data_flows(args.subscription, args.resource_group, args.factory, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

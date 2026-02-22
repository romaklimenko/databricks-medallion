#!/usr/bin/env python3
"""Generate databricks.yml from template and .env file."""

import os
import re
import sys
from pathlib import Path


def load_env(env_path: Path) -> dict[str, str]:
    """Load environment variables from .env file."""
    env_vars: dict[str, str] = {}
    if not env_path.exists():
        return env_vars

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars


def substitute_vars(content: str, env_vars: dict[str, str]) -> str:
    """Replace ${VAR} placeholders with values from env_vars."""

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        if var_name in env_vars:
            return env_vars[var_name]
        # Keep original if not found
        return match.group(0)

    return re.sub(r"\$\{(\w+)\}", replacer, content)


def main() -> int:
    root = Path(__file__).parent.parent
    template_path = root / "databricks.yml.tmpl"
    output_path = root / "databricks.yml"
    env_path = root / ".env"

    if not template_path.exists():
        print(f"Error: Template not found: {template_path}", file=sys.stderr)
        return 1

    # Load .env if exists
    env_vars = load_env(env_path)
    if not env_vars:
        print("Warning: No .env file found or empty. Using environment variables.")
        env_vars = dict(os.environ)

    # Read template
    template_content = template_path.read_text()

    # Substitute variables
    output_content = substitute_vars(template_content, env_vars)

    # Check for unsubstituted variables
    remaining = re.findall(r"\$\{(\w+)\}", output_content)
    if remaining:
        print(f"Warning: Unsubstituted variables: {', '.join(set(remaining))}")

    # Write output
    output_path.write_text(output_content)
    print(f"Generated: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

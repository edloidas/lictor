#!/usr/bin/env bash
# Lists the project's own check commands and whether each can run here, where there is
# no network: `ready <command>` or `blocked <command> (<reason>)`, one per line. Runs
# nothing; it only reads manifests from the current directory.
set -euo pipefail

found=0
report() {
  found=1
  echo "$@"
}

has() { command -v "$1" >/dev/null 2>&1; }

if [ -f package.json ]; then
  runner=npm
  if [ -f bun.lock ] || [ -f bun.lockb ]; then
    runner=bun
  elif [ -f pnpm-lock.yaml ]; then
    runner=pnpm
  elif [ -f yarn.lock ]; then
    runner=yarn
  fi

  scripts=""
  if has bun; then
    scripts=$(bun -e 'const s = (await Bun.file("package.json").json()).scripts ?? {}; console.log(Object.keys(s).join("\n"))' 2>/dev/null || true)
  elif has node; then
    scripts=$(node -e 'const s = require("./package.json").scripts || {}; console.log(Object.keys(s).join("\n"))' 2>/dev/null || true)
  else
    # No runtime to parse JSON with: read the keys of the first `"scripts": { … }` object,
    # whose values are strings and so hold no brace of their own.
    if grep -q '"scripts"[[:space:]]*:' package.json; then
      scripts=$(tr -d '\n' <package.json |
        sed -E 's/.*"scripts"[[:space:]]*:[[:space:]]*\{([^}]*)\}.*/\1/' |
        grep -Eo '"(typecheck|check|lint|test)"[[:space:]]*:' |
        sed -E 's/^"([a-z]+)".*/\1/' || true)
    fi
  fi

  for name in typecheck check lint test; do
    printf '%s\n' "$scripts" | grep -qx "$name" || continue
    command="$runner run $name"
    if ! has "$runner"; then
      report "blocked $command ($runner is not installed)"
    elif [ ! -d node_modules ]; then
      report "blocked $command (node_modules absent and nothing can be installed offline)"
    else
      report "ready $command"
    fi
  done
fi

if [ -f Makefile ] || [ -f makefile ]; then
  for target in check lint test; do
    grep -Eq "^$target:" Makefile makefile 2>/dev/null || continue
    if has make; then
      report "ready make $target"
    else
      report "blocked make $target (make is not installed)"
    fi
  done
fi

if [ -f Cargo.toml ]; then
  if ! has cargo; then
    report "blocked cargo test --offline (cargo is not installed)"
  elif [ -d vendor ] || [ -d "${CARGO_HOME:-$HOME/.cargo}/registry" ]; then
    report "ready cargo test --offline"
  else
    report "blocked cargo test --offline (no crate registry or vendor directory)"
  fi
fi

if [ -f go.mod ]; then
  if ! has go; then
    report "blocked go test ./... (go is not installed)"
  elif [ -d vendor ] || [ -d "$(go env GOMODCACHE 2>/dev/null || echo /nonexistent)" ]; then
    report "ready go test ./..."
  else
    report "blocked go test ./... (no module cache or vendor directory)"
  fi
fi

if [ -f pyproject.toml ] || [ -f pytest.ini ] || [ -f setup.cfg ]; then
  if has pytest; then
    report "ready pytest"
  else
    report "blocked pytest (pytest is not installed)"
  fi
fi

[ "$found" -eq 1 ] || echo "none (no check command declared in a manifest this script reads)"

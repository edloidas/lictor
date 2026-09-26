#!/usr/bin/env bash
# Resolves what a review covers: the commit to diff against, the reviewable files,
# their size, and a suggested depth. Prints key=value lines, then "== files" and one
# "<added>\t<deleted>\t<path>" line per reviewable file. Reads git only; writes nothing.
#
#   scope.sh <base-ref>                  whole change: diff from merge-base(base, HEAD)
#   scope.sh --since <commit> <base-ref> re-review: diff from <commit>
set -euo pipefail

usage() {
  echo "usage: scope.sh [--since <commit>] <base-ref>" >&2
  exit 2
}

since=""
base=""
while [ $# -gt 0 ]; do
  case "$1" in
    --since)
      [ $# -ge 2 ] || usage
      since="$2"
      shift 2
      ;;
    -h | --help) usage ;;
    -*) usage ;;
    *)
      [ -z "$base" ] || usage
      base="$1"
      shift
      ;;
  esac
done
[ -n "$base" ] || usage

resolve() {
  git rev-parse --verify --quiet "$1^{commit}" 2>/dev/null ||
    git rev-parse --verify --quiet "origin/$1^{commit}" 2>/dev/null ||
    true
}

head=$(git rev-parse HEAD)
base_sha=$(resolve "$base")
if [ -z "$base_sha" ]; then
  echo "scope.sh: base '$base' does not resolve to a commit" >&2
  exit 1
fi

merge_base=$(git merge-base "$base_sha" "$head")
if [ -n "$since" ]; then
  from=$(resolve "$since")
  if [ -z "$from" ]; then
    echo "scope.sh: commit '$since' is not in this clone" >&2
    exit 1
  fi
  if ! git merge-base --is-ancestor "$from" "$head"; then
    echo "scope.sh: '$since' is not an ancestor of HEAD — the branch was rewritten" >&2
    exit 1
  fi
  from_kind=since
else
  from=$merge_base
  from_kind=merge-base
fi

# Not reviewed line by line. Kept apart so a skipped change is named for what it is:
# `build/`, `out/` and `vendor/` hold source often enough that they stay reviewable.
lock_pattern='(^|/)[^/]*-lock\.(json|yaml)$|(^|/)(bun\.lockb?|yarn\.lock|Cargo\.lock|Gemfile\.lock|poetry\.lock|composer\.lock|go\.sum|uv\.lock)$'
generated_pattern='(^|/)(node_modules|dist|coverage|\.next)/|\.min\.(js|css)$|\.map$'
skip_pattern="$lock_pattern|$generated_pattern"

numstat=$(git diff --numstat --no-renames --no-color "$from" "$head")
reviewable=""
skipped=0
if [ -n "$numstat" ]; then
  reviewable=$(printf '%s\n' "$numstat" | SKIP="$skip_pattern" awk -F '\t' '$3 !~ ENVIRON["SKIP"]')
  skipped=$(printf '%s\n' "$numstat" | SKIP="$skip_pattern" awk -F '\t' '$3 ~ ENVIRON["SKIP"]' | wc -l | tr -d ' ')
fi

files=0
added=0
deleted=0
if [ -n "$reviewable" ]; then
  # Binary files report "-" for both counts and add no lines.
  read -r files added deleted <<EOF
$(printf '%s\n' "$reviewable" | awk -F '\t' '{ n++; if ($1 != "-") a += $1; if ($2 != "-") d += $2 } END { printf "%d %d %d\n", n, a, d }')
EOF
fi
lines=$((added + deleted))

trivial=no
if [ -z "$numstat" ]; then
  trivial=empty
elif [ "$files" -eq 0 ]; then
  if printf '%s\n' "$numstat" | LOCK="$lock_pattern" awk -F '\t' '$3 !~ ENVIRON["LOCK"] { found = 1 } END { exit !found }'; then
    trivial=generated
  else
    trivial=lockfiles
  fi
fi

if [ "$lines" -le 150 ] && [ "$files" -le 5 ]; then
  mode=quick
elif [ "$lines" -gt 1500 ] || [ "$files" -gt 30 ]; then
  mode=deep
else
  mode=standard
fi

echo "head=$head"
echo "from=$from"
echo "from_kind=$from_kind"
echo "merge_base=$merge_base"
echo "files=$files"
echo "added=$added"
echo "deleted=$deleted"
echo "lines=$lines"
echo "skipped=$skipped"
echo "trivial=$trivial"
echo "mode=$mode"
echo "== files"
[ -z "$reviewable" ] || printf '%s\n' "$reviewable"

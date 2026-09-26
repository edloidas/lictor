#!/usr/bin/env bash
# Checks review-comment anchors against the diff GitHub will accept them on: a line
# inside a hunk of `git diff <from> HEAD` with three lines of context, on the side named.
# One anchor per stdin line as `path:line` or `path:line:LEFT|RIGHT` (RIGHT by default).
# Prints `ok <anchor>` or `no <anchor> (hunks: a-b c-d)` per line; exits 1 if any is `no`.
#
#   printf '%s\n' 'src/a.ts:42' 'src/b.ts:7:LEFT' | anchors.sh <from>
set -euo pipefail

if [ $# -ne 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  echo "usage: anchors.sh <from> < anchors" >&2
  exit 2
fi
from="$1"
if ! git rev-parse --verify --quiet "$from^{commit}" >/dev/null; then
  echo "anchors.sh: '$from' does not resolve to a commit" >&2
  exit 2
fi

# Prints one "<side> <first> <last>" line per hunk of <path> on each side.
hunks() {
  git diff --no-color --no-ext-diff -U3 "$from" HEAD -- "$1" | awk '
    /^@@ / {
      # Only the two range fields: text after the closing @@ is source, and may hold "-1".
      split($0, part, " ")
      for (i = 2; i <= 3; i++) {
        field = part[i]
        side = substr(field, 1, 1)
        if (side != "-" && side != "+") continue
        field = substr(field, 2)
        count = 1
        if (index(field, ",") > 0) {
          split(field, range, ",")
          start = range[1] + 0
          count = range[2] + 0
        } else {
          start = field + 0
        }
        if (count > 0) print (side == "-" ? "LEFT" : "RIGHT"), start, start + count - 1
      }
    }'
}

status=0
while IFS= read -r anchor || [ -n "$anchor" ]; do
  [ -n "$anchor" ] || continue
  side=RIGHT
  rest="$anchor"
  case "$rest" in
    *:LEFT) side=LEFT; rest="${rest%:LEFT}" ;;
    *:RIGHT) rest="${rest%:RIGHT}" ;;
  esac
  line="${rest##*:}"
  path="${rest%:*}"
  case "$line" in
    '' | *[!0-9]*)
      echo "no $anchor (not path:line)"
      status=1
      continue
      ;;
  esac
  if [ "$path" = "$rest" ] || [ -z "$path" ]; then
    echo "no $anchor (not path:line)"
    status=1
    continue
  fi

  if ! found=$(hunks "$path" 2>/dev/null); then
    echo "no $anchor (git cannot diff this path)"
    status=1
    continue
  fi
  ranges=$(printf '%s\n' "$found" | awk -v s="$side" '$1 == s { print $2 "-" $3 }' | tr '\n' ' ')
  inside=$(printf '%s\n' "$found" | awk -v s="$side" -v l="$line" '$1 == s && l >= $2 && l <= $3 { print "yes"; exit }')
  if [ "$inside" = "yes" ]; then
    echo "ok $anchor"
  elif [ -z "$ranges" ]; then
    echo "no $anchor (no $side hunks in this diff)"
    status=1
  else
    echo "no $anchor (hunks: ${ranges% })"
    status=1
  fi
done

exit "$status"

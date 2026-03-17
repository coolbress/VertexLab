#!/usr/bin/env bash
set -euo pipefail
MODE="dry-run"
SEARCH=""
LIMIT="200"
PARALLEL="8"
while [ $# -gt 0 ]; do
  case "$1" in
    --mode)
      if [ $# -lt 2 ] || [ -z "${2-}" ] || [[ "$2" == -* ]]; then
        echo "Missing or invalid value for --mode" >&2; exit 1
      fi
      MODE="$2"; shift 2;;
    --search)
      if [ $# -lt 2 ] || [ -z "${2-}" ] || [[ "$2" == -* ]]; then
        echo "Missing or invalid value for --search" >&2; exit 1
      fi
      SEARCH="$2"; shift 2;;
    --limit)
      if [ $# -lt 2 ] || [ -z "${2-}" ] || [[ "$2" == -* ]]; then
        echo "Missing or invalid value for --limit" >&2; exit 1
      fi
      LIMIT="$2"; shift 2;;
    --parallel)
      if [ $# -lt 2 ] || [ -z "${2-}" ] || [[ "$2" == -* ]]; then
        echo "Missing or invalid value for --parallel" >&2; exit 1
      fi
      PARALLEL="$2"; shift 2;;
    *)
      echo "Unknown option: $1" >&2; exit 1;;
  esac
done
if ! command -v gh >/dev/null 2>&1; then
  echo "gh not found" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found" >&2
  exit 1
fi
repo="${GH_REPO:-}"
if [ -z "$repo" ]; then
  repo="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
fi
query=(gh pr list --repo "$repo" --state merged --limit "$LIMIT" --json number,title)
if [ -n "$SEARCH" ]; then
  query+=(--search "$SEARCH")
fi
prs_json="$("${query[@]}")"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
echo "$prs_json" | jq -r '.[] | "\(.number)\t\(.title)"' > "$tmpdir/pr_list.tsv"
# prefetch files in parallel to reduce API round-trip latency
export repo tmpdir
cut -f1 "$tmpdir/pr_list.tsv" | xargs -I{} -P "$PARALLEL" sh -c '
  set -e
  num="$1"
  # capture output; do not create file on failure/empty
  out="$(gh pr view "$num" --repo "$repo" --json files --jq ".files[].path" 2>/dev/null || true)"
  if [ -n "$out" ]; then
    printf "%s\n" "$out" > "$tmpdir/$num.files"
  fi
' _ {}
# iterate and label
while IFS=$'\t' read -r num title; do
  t="$(echo "$title" | tr '[:upper:]' '[:lower:]' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  typ=""
  if echo "$t" | grep -Eq '^(feat)(\(|!|:)' ; then
    typ="type:feature"
  elif echo "$t" | grep -Eq '^(fix)(\(|!|:)' ; then
    typ="type:fix"
  elif echo "$t" | grep -Eq '^(docs)(\(|!|:)' ; then
    typ="type:docs"
  elif echo "$t" | grep -Eq '^(chore)(\(|!|:)' ; then
    typ="type:chore"
  elif echo "$t" | grep -Eq '^(refactor)(\(|!|:)' ; then
    typ="type:refactor"
  elif echo "$t" | grep -Eq '^(perf)(\(|!|:)' ; then
    typ="type:perf"
  elif echo "$t" | grep -Eq '^(test)(\(|!|:)' ; then
    typ="type:test"
  fi
  # skip PRs whose files could not be fetched
  if [ ! -s "$tmpdir/$num.files" ]; then
    echo "PR #$num: skipped (no files or fetch failed)"
    continue
  fi
  files="$(cat "$tmpdir/$num.files")"
  scopes=()
  if echo "$files" | grep -Eq '(^|/)docs/|^README\.md$' ; then scopes+=("scope:docs"); fi
  if echo "$files" | grep -Eq 'packages/vertex-forager/src/vertex_forager/core/|packages/vertex-forager/src/vertex_forager/routers/' ; then scopes+=("scope:pipeline"); fi
  if echo "$files" | grep -Eq 'packages/vertex-forager/src/vertex_forager/writers/' ; then scopes+=("scope:writer"); fi
  if echo "$files" | grep -Eq 'packages/vertex-forager/src/vertex_forager/providers/yfinance/' ; then scopes+=("scope:provider:yfinance"); fi
  if echo "$files" | grep -Eq 'packages/vertex-forager/src/vertex_forager/providers/sharadar/' ; then scopes+=("scope:provider:sharadar"); fi
  if echo "$files" | grep -Eq 'packages/vertex-forager/src/vertex_forager/providers/' ; then scopes+=("area:providers"); fi
  if echo "$files" | grep -Eq '(^|/)\.github/' ; then scopes+=("scope:ci"); fi
  uniq_labels=()
  for l in "$typ" "${scopes[@]}"; do
    [ -n "$l" ] || continue
    skip=""
    for e in "${uniq_labels[@]:-}"; do
      [ "$e" = "$l" ] && skip="1" && break
    done
    [ -n "$skip" ] || uniq_labels+=("$l")
  done
  if [ "${#uniq_labels[@]}" -eq 0 ]; then
    echo "PR #$num: no labels inferred"
    continue
  fi
  if [ "$MODE" = "apply" ]; then
    gh pr edit "$num" --repo "$repo" --add-label "$(IFS=, ; echo "${uniq_labels[*]}")"
    echo "PR #$num: applied labels: ${uniq_labels[*]}"
  else
    echo "PR #$num: would apply labels: ${uniq_labels[*]}"
  fi
done < "$tmpdir/pr_list.tsv"

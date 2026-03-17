#!/usr/bin/env bash
set -euo pipefail
MODE="dry-run"
SEARCH=""
LIMIT="200"
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE="$2"; shift 2;;
    --search) SEARCH="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    *) shift;;
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
echo "$prs_json" | jq -r '.[] | "\(.number)\t\(.title)"' | while IFS=$'\t' read -r num title; do
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
  files="$(gh pr view "$num" --repo "$repo" --json files --jq '.files[].path')"
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
done

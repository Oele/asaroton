#!/usr/bin/env bash
# The asaroton watch job: collect, analyse, publish. Started by cron; see README.md.
#
#   watch.sh                      a full run
#   watch.sh --dry-run            analyse and print the digest; file nothing, post nothing, keep cursors
#   watch.sh --skip complement    leave a source out (comma separated)
#   watch.sh --resume RUN_DIR     reuse material already collected in RUN_DIR
set -uo pipefail
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
export LANG=C.UTF-8
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(dirname "$HERE")
STATE=${ASAROTON_WATCH_STATE:-$HOME/.local/state/asaroton-watch}
DRY=""; SKIP=""; RUN=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY=--dry-run ;;
    --skip) SKIP=$2; shift ;;
    --resume) RUN=$2; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
mkdir -p "$STATE/runs"
exec 9>"$STATE/lock"
flock -n 9 || { echo "asaroton watch: a run is already in progress" >&2; exit 0; }

RESUMED=$RUN
[ -n "$RUN" ] || RUN="$STATE/runs/$(date +%Y-%m-%d_%H%M)"
mkdir -p "$RUN"
LOG="$RUN/watch.log"
cfg() { python3 -c "import tomllib,sys; d=tomllib.load(open('$HERE/config.toml','rb')); print(d$1)"; }

fail() {
  echo "asaroton watch failed: $1" | tee -a "$LOG" >&2
  tail -n 15 "$LOG" >&2
  [ -n "$DRY" ] || python3 "$HERE/publish.py" "$RUN" --failure "$1" >>"$LOG" 2>&1
  exit 1
}

echo "== run $(date -Is) $DRY skip=$SKIP" >>"$LOG"
if [ -z "$RESUMED" ]; then
  python3 "$HERE/collect.py" "$RUN" ${SKIP:+--skip "$SKIP"} >>"$LOG" 2>&1 || fail "the collector crashed; see $LOG"
fi
[ -s "$RUN/status.json" ] || fail "nothing was collected"

SINCE=$(python3 -c "import json; print(json.load(open('$RUN/status.json'))['since'][:10])")
PROMPT=$(sed -e "s|{repo}|$REPO|g" -e "s|{run_dir}|$RUN|g" -e "s|{since}|$SINCE|g" -e "s|{until}|$(date +%Y-%m-%d)|g" \
             -e "s|{max_findings}|$(( $(cfg "['issues']['max_new_per_run']") + 10 ))|g" "$HERE/analyst-prompt.md")
MODEL=$(cfg "['analyst']['model']")
cd "$REPO"
# The analyst can read files and nothing else: no shell, no network, no editing.
timeout "$(cfg "['analyst']['timeout_minutes']")m" claude -p "$PROMPT" \
  --tools "Read,Grep,Glob" --allowedTools "Read Grep Glob" --add-dir "$RUN" --strict-mcp-config \
  --output-format json --json-schema "$(cat "$HERE/findings.schema.json")" \
  --max-budget-usd "$(cfg "['analyst']['max_budget_usd']")" ${MODEL:+--model "$MODEL"} \
  >"$RUN/analysis-raw.json" 2>>"$LOG"
RC=$?
python3 - "$RUN" >>"$LOG" 2>&1 <<'PY' || fail "the analysis did not return findings (claude exit $RC); see $RUN/analysis-raw.json"
import json, sys, pathlib
run = pathlib.Path(sys.argv[1])
raw = json.loads((run / "analysis-raw.json").read_text())
out = raw.get("structured_output")
assert not raw.get("is_error") and isinstance(out, dict) and "findings" in out, raw.get("result", "")[:500]
(run / "analysis.json").write_text(json.dumps(out, indent=1, ensure_ascii=False))
print(f"analysis: {len(out['findings'])} findings, ${raw.get('total_cost_usd', 0):.2f}, {raw.get('num_turns')} turns")
PY

python3 "$HERE/publish.py" "$RUN" $DRY >>"$LOG" 2>"$RUN/publish.err" || { cat "$RUN/publish.err" >>"$LOG"; fail "publishing failed; see $LOG"; }
cat "$RUN/publish.err" >>"$LOG"
if [ -n "$DRY" ]; then
  cat "$RUN/digest.md"
else
  # Only now move the cursors, so a failed run is repeated in full next time.
  cp "$RUN/next_state.json" "$STATE/state.json"
  [ -s "$RUN/complement-results.json" ] && cp "$RUN/complement-results.json" "$STATE/complement-baseline.json"
  ls -1d "$STATE"/runs/*/ 2>/dev/null | head -n -12 | xargs -r rm -rf
fi
echo "== done $(date -Is)" >>"$LOG"

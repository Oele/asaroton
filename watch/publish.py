#!/usr/bin/env python3
"""Turns the analyst's findings into GitHub issues and posts the digest to the Matrix room.

The model never touches GitHub or Matrix itself: it returns data, and this script decides what
gets written where. A finding becomes a new issue unless an issue with the same watch-key (or
the issue number the analyst named) already exists; then new evidence is added as a comment.

    publish.py RUN_DIR [--dry-run]
    publish.py RUN_DIR --failure "what went wrong"
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import sys
import time
import tomllib
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
CONFIG_DIR = pathlib.Path(os.environ.get("TESSERA_WATCH_CONFIG", "~/.config/tessera-watch")).expanduser()
SEVERITY = {"high": 0, "medium": 1, "low": 2}
CONFIDENCE = {"confirmed": 0, "likely": 1, "unverified": 2}
KIND_LABEL = {"bug": "bug", "security": "security", "spec-gap": "enhancement", "test-failure": "bug",
              "interop": "bug", "ops": "ops", "dependency": "dependencies"}
LABELS = {
    "watch": ("5319e7", "Filed by the watch job"),
    "security": ("b60205", "Security relevant"),
    "ops": ("fbca04", "Running the servers"),
    "dependencies": ("0366d6", "Dependency update"),
    "severity:high": ("b60205", ""), "severity:medium": ("d93f0b", ""), "severity:low": ("c5def5", ""),
}


def gh(*args, check=True, stdin=None):
    p = subprocess.run(["gh", *args], capture_output=True, text=True, input=stdin, timeout=120)
    if check and p.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {p.stderr.strip()[-400:]}")
    return p.stdout.strip()


def to_html(md):
    try:
        import markdown
        return markdown.markdown(md, extensions=["sane_lists"])
    except ImportError:
        html = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", md.replace("&", "&amp;").replace("<", "&lt;"))
        html = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r'<a href="\2">\1</a>', html)
        return html.replace("\n", "<br>")


def send_matrix(config, text):
    secrets = tomllib.loads((CONFIG_DIR / "secrets.toml").read_text())
    room = urllib.parse.quote(secrets["room_id"])
    # Stay well below the 65 KB event limit; a digest that long is cut into parts at paragraph ends.
    parts, current = [], ""
    for para in text.split("\n\n"):
        if len(current) + len(para) > 12000 and current:
            parts.append(current)
            current = ""
        current += para + "\n\n"
    parts.append(current)
    for n, part in enumerate(parts):
        body = {"msgtype": "m.text", "body": part.strip(), "format": "org.matrix.custom.html", "formatted_body": to_html(part.strip())}
        url = f"{config['matrix']['homeserver']}/_matrix/client/v3/rooms/{room}/send/m.room.message/watch{int(time.time() * 1000)}{n}"
        req = urllib.request.Request(url, data=json.dumps(body).encode(), method="PUT",
                                     headers={"Authorization": f"Bearer {secrets['access_token']}", "Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=60).read()


def source_summary(run_dir):
    status = json.loads((run_dir / "status.json").read_text()) if (run_dir / "status.json").exists() else {"sources": {}}
    ok = [n for n, s in status["sources"].items() if s == "ok"]
    bad = [f"{n} ({s})" for n, s in status["sources"].items() if s != "ok"]
    line = f"Sources read: {', '.join(ok) or 'none'}."
    if bad:
        line += f" **Not read: {'; '.join(b[:160] for b in bad)}.**"
    hosts = run_dir / "material" / "hosts.md"
    if hosts.exists() and "restricted key not accepted" in hosts.read_text():
        line += " cluephone.org was read through an interactive ssh session; the restricted key is not installed there yet."
    return line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=pathlib.Path)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--failure")
    args = ap.parse_args()
    config = tomllib.loads((HERE / "config.toml").read_text())
    repo = config["issues"]["repo"]
    today = dt.date.today()

    if args.failure:
        text = f"**tessera watch, {today:%-d %b %Y}: the run failed**\n\n{args.failure}\n\nNothing was filed. The run directory is `{args.run_dir}`."
        (args.run_dir / "digest.md").write_text(text)
        if not args.dry_run:
            send_matrix(config, text)
        return

    analysis = json.loads((args.run_dir / "analysis.json").read_text())
    existing = json.loads(gh("issue", "list", "-R", repo, "--state", "all", "--limit", "500", "--json", "number,title,state,body,url"))
    by_number = {i["number"]: i for i in existing}
    by_key = {}
    for i in existing:
        m = re.search(r"<!-- watch-key: (\S+) -->", i.get("body") or "")
        if m:
            by_key[m.group(1)] = i
    if not args.dry_run:
        for name, (colour, desc) in LABELS.items():
            gh("label", "create", name, "-R", repo, "--color", colour, "--description", desc, "--force", check=False)
        for src in sorted({f["source"] for f in analysis["findings"]}):
            gh("label", "create", f"source:{src}", "-R", repo, "--color", "ededed", "--force", check=False)

    findings = sorted(analysis["findings"], key=lambda f: (SEVERITY[f["severity"]], CONFIDENCE[f["confidence"]]))
    limit = config["issues"]["max_new_per_run"]
    created, updated, known, unfiled = [], [], [], []
    for f in findings:
        issue = by_key.get(f["key"]) or by_number.get(f.get("existing_issue"))
        if issue:
            if issue["state"].lower() == "open" and f.get("comment"):
                if not args.dry_run:
                    gh("issue", "comment", str(issue["number"]), "-R", repo, "--body-file", "-",
                       stdin=f"Watch run of {today}:\n\n{f['comment']}")
                updated.append((issue, f))
            else:
                known.append((issue, f))
            continue
        if len(created) >= limit:
            unfiled.append(f)
            continue
        body = (f"{f['body'].strip()}\n\n---\nFiled by the watch job on {today}. Source: {f['source']}. "
                f"Confidence: {f['confidence']}.\n<!-- watch-key: {f['key']} -->\n")
        labels = ["watch", f"source:{f['source']}", f"severity:{f['severity']}", KIND_LABEL[f["kind"]]]
        if args.dry_run:
            url = "(dry run)"
        else:
            url = gh("issue", "create", "-R", repo, "--title", f["title"][:200], "--body-file", "-",
                     "--label", ",".join(dict.fromkeys(labels)), stdin=body).splitlines()[-1]
        created.append((url, f))

    since = json.loads((args.run_dir / "status.json").read_text())["since"][:10]
    out = [f"**tessera watch, {since} to {today}**", analysis["headline"].strip(), analysis["digest"].strip()]
    if created:
        out.append(f"**New issues ({len(created)})**\n" + "\n".join(
            f"- [{f['title']}]({url}) · {f['severity']}, {f['confidence']}" for url, f in created))
    if updated:
        out.append(f"**New evidence on open issues ({len(updated)})**\n" + "\n".join(
            f"- [#{i['number']} {i['title']}]({i['url']})" for i, f in updated))
    if known:
        out.append(f"**Seen again, already tracked ({len(known)})**\n" + "\n".join(
            f"- [#{i['number']} {i['title']}]({i['url']}) ({i['state'].lower()})" for i, f in known))
    if unfiled:
        out.append(f"**Not filed, over the limit of {limit} per run ({len(unfiled)})**\n" + "\n".join(
            f"- {f['title']} · {f['severity']}" for f in unfiled))
    if analysis.get("watching"):
        out.append("**Watching**\n" + "\n".join(f"- {w}" for w in analysis["watching"]))
    if not (created or updated or unfiled):
        out.append("No new findings this run.")
    out.append(source_summary(args.run_dir))
    text = "\n\n".join(out)
    (args.run_dir / "digest.md").write_text(text + "\n")
    (args.run_dir / "published.json").write_text(json.dumps({
        "created": [u for u, _ in created], "updated": [i["number"] for i, _ in updated],
        "unfiled": [f["key"] for f in unfiled]}, indent=1))
    if args.dry_run:
        print(text)
    else:
        send_matrix(config, text)
    print(f"published: {len(created)} new, {len(updated)} updated, {len(known)} known, {len(unfiled)} unfiled", file=sys.stderr)


if __name__ == "__main__":
    main()

"""Resolve release routing from the PR that produced a main push."""

import json
import os
from pathlib import Path
import subprocess
import time


def resolve_merged_pr(repository, sha):
    """Wait for GitHub to expose exactly one merged PR for this commit."""
    for attempt in range(6):
        pages = json.loads(subprocess.check_output([
            "gh", "api", "--paginate", "--slurp",
            f"repos/{repository}/commits/{sha}/pulls",
        ], text=True))
        matches = [
            pr for page in pages for pr in page
            if pr["merged_at"] is not None
            and pr["merge_commit_sha"] == sha
            and pr["base"]["ref"] == "main"
            and pr["base"]["repo"]["full_name"] == repository
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise RuntimeError("Multiple merged PRs match the pushed commit")
        # The push can arrive before the pull-request API reflects the merge.
        if attempt < 5:
            time.sleep(5)
    raise RuntimeError("No merged PR matches the pushed commit; retry after GitHub records the merge")


def main():
    """Write routing outputs only for a verified main-push merge."""
    if os.environ["GITHUB_EVENT_NAME"] != "push" or os.environ["GITHUB_REF"] != "refs/heads/main":
        raise RuntimeError("Merged PR resolution requires a push to main")
    pr = resolve_merged_pr(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"])
    head_ref = pr["head"]["ref"]
    if not head_ref or "\n" in head_ref or "\r" in head_ref:
        raise RuntimeError("Invalid PR source branch")
    release = any(label["name"] == "release" for label in pr["labels"])
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"release={str(release).lower()}\nhead-ref={head_ref}\n")


if __name__ == "__main__":
    main()

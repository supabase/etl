"""Check release routing against unrelated, ambiguous, and delayed PR metadata."""

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from resolve import main, resolve_merged_pr


SHA = "a" * 40
REPOSITORY = "example/etl"
PR = {
    "merged_at": "2026-01-01T00:00:00Z",
    "merge_commit_sha": SHA,
    "base": {"ref": "main", "repo": {"full_name": REPOSITORY}},
    "head": {"ref": "fix/example"},
    "labels": [],
}


class RoutingTests(unittest.TestCase):
    """Exercise the same resolver used by ordinary and release publishing."""

    @patch("resolve.subprocess.check_output")
    def test_only_the_exact_main_merge_selects_the_route(self, api):
        other = copy.deepcopy(PR)
        other["merge_commit_sha"] = "b" * 40
        other["labels"] = [{"name": "release"}]
        api.return_value = json.dumps([[other], [PR]])
        self.assertEqual(resolve_merged_pr(REPOSITORY, SHA), PR)
        self.assertIn("--paginate", api.call_args.args[0])

    @patch("resolve.time.sleep")
    @patch("resolve.subprocess.check_output")
    def test_waits_for_the_merge_to_be_visible(self, api, sleep):
        unmerged = dict(PR, merged_at=None)
        api.side_effect = [json.dumps([[unmerged]]), json.dumps([[PR]])]
        self.assertEqual(resolve_merged_pr(REPOSITORY, SHA), PR)
        sleep.assert_called_once_with(5)

    @patch("resolve.time.sleep")
    @patch("resolve.subprocess.check_output")
    def test_missing_or_wrong_base_fails_instead_of_publishing(self, api, sleep):
        wrong_branch = copy.deepcopy(PR)
        wrong_branch["base"]["ref"] = "other"
        wrong_repo = copy.deepcopy(PR)
        wrong_repo["base"]["repo"]["full_name"] = "other/etl"
        api.return_value = json.dumps([[wrong_branch, wrong_repo]])
        with self.assertRaisesRegex(RuntimeError, "No merged PR"):
            resolve_merged_pr(REPOSITORY, SHA)
        self.assertEqual(api.call_count, 6)
        self.assertEqual(sleep.call_count, 5)

    @patch("resolve.subprocess.check_output", return_value=json.dumps([[PR, PR]]))
    def test_ambiguous_merges_fail(self, api):
        with self.assertRaisesRegex(RuntimeError, "Multiple merged PRs"):
            resolve_merged_pr(REPOSITORY, SHA)

    @patch("resolve.resolve_merged_pr")
    def test_outputs_preserve_ordinary_and_release_routing(self, resolve):
        for release in [False, True]:
            with self.subTest(release=release), tempfile.TemporaryDirectory() as directory:
                pr = copy.deepcopy(PR)
                if release:
                    pr["labels"] = [{"name": "release"}]
                    pr["head"]["ref"] = "release/v1.2.3"
                resolve.return_value = pr
                output = Path(directory) / "output"
                with patch.dict(os.environ, {
                    "GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main",
                    "GITHUB_REPOSITORY": REPOSITORY, "GITHUB_SHA": SHA,
                    "GITHUB_OUTPUT": str(output),
                }):
                    main()
                self.assertEqual(output.read_text(),
                                 f"release={str(release).lower()}\nhead-ref={pr['head']['ref']}\n")

    @patch("resolve.resolve_merged_pr")
    def test_malformed_branch_cannot_inject_an_output(self, resolve):
        pr = copy.deepcopy(PR)
        pr["head"]["ref"] = "fix/example\nrelease=true"
        resolve.return_value = pr
        with patch.dict(os.environ, {
            "GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": REPOSITORY, "GITHUB_SHA": SHA,
        }):
            with self.assertRaisesRegex(RuntimeError, "Invalid PR source branch"):
                main()

    @patch("resolve.resolve_merged_pr")
    def test_manual_and_non_main_events_cannot_select_stable_routing(self, resolve):
        for event, ref in [("workflow_dispatch", "refs/heads/main"),
                           ("pull_request", "refs/heads/main"),
                           ("push", "refs/heads/feature")]:
            with self.subTest(event=event, ref=ref), patch.dict(os.environ, {
                "GITHUB_EVENT_NAME": event, "GITHUB_REF": ref,
            }):
                with self.assertRaisesRegex(RuntimeError, "push to main"):
                    main()
        resolve.assert_not_called()


if __name__ == "__main__":
    unittest.main()

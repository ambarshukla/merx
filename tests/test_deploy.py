"""The deploy job's guards, checked as data rather than trusted to review.

Only two properties matter enough to test: nothing reaches the host unless the tests
passed, and nothing reaches it from a branch. Both are one line of YAML and both are
easy to lose in an edit.
"""

from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def test_deploy_waits_for_the_tests():
    assert _jobs()["deploy"]["needs"] == "test"


def test_deploy_only_from_main():
    assert _jobs()["deploy"]["if"] == "github.ref == 'refs/heads/main'"


def test_deploys_do_not_overlap():
    concurrency = _jobs()["deploy"]["concurrency"]
    assert concurrency["group"]
    assert concurrency["cancel-in-progress"] is False

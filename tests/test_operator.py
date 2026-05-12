"""Test GreenNodeOperator argument normalization và validation."""

import json

import pytest

from greennode_airflow_plugin.greennode_operator import GreenNodeOperator


def _op(**kwargs):
    return GreenNodeOperator(task_id="t", workspace_id="ws", job_id="job", **kwargs)


def test_required_workspace_id():
    with pytest.raises(Exception):
        GreenNodeOperator(task_id="t", workspace_id="", job_id="job")


def test_required_job_id():
    with pytest.raises(Exception):
        GreenNodeOperator(task_id="t", workspace_id="ws", job_id="")


def test_normalize_args_none():
    # None must produce an EMPTY list, not a single blank token —
    # otherwise the Spark driver receives `sys.argv[1] = ""`, which
    # confuses argparse and any script that branches on len(sys.argv).
    assert GreenNodeOperator._normalize_args(None) == []


def test_normalize_args_empty_list():
    assert GreenNodeOperator._normalize_args([]) == []


def test_normalize_args_list():
    assert GreenNodeOperator._normalize_args(["a", "b"]) == ["a", "b"]


def test_normalize_args_dict():
    out = GreenNodeOperator._normalize_args({"k": "v"})
    assert json.loads(out[0]) == {"k": "v"}


def test_normalize_args_json_string_list():
    assert GreenNodeOperator._normalize_args('["a", "b"]') == ["a", "b"]


def test_normalize_args_plain_string():
    assert GreenNodeOperator._normalize_args("foo") == ["foo"]

"""Test SparkJobState enum."""

from greennode_airflow_plugin import SparkJobState


def test_from_str_known():
    assert SparkJobState.from_str("RUNNING") == SparkJobState.RUNNING
    assert SparkJobState.from_str("success") == SparkJobState.SUCCESS


def test_from_str_unknown():
    assert SparkJobState.from_str(None) == SparkJobState.UNKNOWN
    assert SparkJobState.from_str("") == SparkJobState.UNKNOWN
    assert SparkJobState.from_str("WEIRD") == SparkJobState.UNKNOWN


def test_is_final():
    assert SparkJobState.SUCCESS.is_final
    assert SparkJobState.FAILED.is_final
    assert SparkJobState.CANCELLED.is_final
    assert not SparkJobState.RUNNING.is_final
    assert not SparkJobState.PENDING.is_final


def test_is_successful():
    assert SparkJobState.SUCCESS.is_successful
    assert not SparkJobState.FAILED.is_successful
    assert not SparkJobState.RUNNING.is_successful

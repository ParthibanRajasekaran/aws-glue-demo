"""Unit tests for glue_setup control-flow branches."""
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src import glue_setup


def _make_config():
    from src.config import Config

    obj = object.__new__(Config)
    obj.aws_profile = "glue-learner"
    obj.aws_region = "us-east-1"
    obj.aws_account_id = "123456789012"
    obj.S3_RAW_PREFIX = "raw/employees/"
    obj.GLUE_DATABASE_NAME = "employee_db"
    obj.GLUE_CRAWLER_NAME = "employee-csv-crawler"
    return obj


@pytest.mark.unit
def test_create_crawler_reconciles_existing():
    config = _make_config()
    mock_glue = MagicMock()
    error = {"Error": {"Code": "AlreadyExistsException", "Message": "exists"}}
    mock_glue.create_crawler.side_effect = ClientError(error, "CreateCrawler")
    mock_glue.get_crawler.return_value = {"Crawler": {"Schedule": None}}
    mock_session = MagicMock()
    mock_session.client.return_value = mock_glue

    with patch.object(glue_setup, "_session", return_value=mock_session):
        glue_setup.create_crawler(config, "arn:aws:iam::123456789012:role/test")

    mock_glue.update_crawler.assert_called_once()


@pytest.mark.unit
def test_run_crawler_waits_if_already_running():
    config = _make_config()
    mock_glue = MagicMock()
    mock_glue.get_crawler.side_effect = [
        {"Crawler": {"State": "RUNNING"}},
        {"Crawler": {"State": "READY", "LastCrawl": {"Status": "SUCCEEDED"}}},
    ]
    mock_session = MagicMock()
    mock_session.client.return_value = mock_glue

    with patch.object(glue_setup, "_session", return_value=mock_session), patch.object(
        glue_setup.time, "sleep", return_value=None
    ):
        glue_setup.run_crawler(config)

    mock_glue.start_crawler.assert_not_called()


@pytest.mark.unit
def test_create_crawler_raises_when_schedule_present():
    config = _make_config()
    mock_glue = MagicMock()
    error = {"Error": {"Code": "AlreadyExistsException", "Message": "exists"}}
    mock_glue.create_crawler.side_effect = ClientError(error, "CreateCrawler")
    mock_glue.get_crawler.return_value = {"Crawler": {"Schedule": {"ScheduleExpression": "cron(...)"}}
    }
    mock_session = MagicMock()
    mock_session.client.return_value = mock_glue

    with patch.object(glue_setup, "_session", return_value=mock_session):
        with pytest.raises(glue_setup.CrawlerConfigurationError):
            glue_setup.create_crawler(config, "arn:aws:iam::123456789012:role/test")

#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from datetime import timedelta
from http import HTTPStatus
from unittest.mock import MagicMock

import mock
import pendulum
import pytest
from airbyte_cdk.models import ConfiguredAirbyteCatalog, SyncMode, Type
from source_hubspot_history_deals.errors import HubspotRateLimited, InvalidStartDateConfigError
from source_hubspot_history_deals.helpers import APIv3Property
from source_hubspot_history_deals.source import SourceHubspotHistoryDeals
from source_hubspot_history_deals.streams import API, Deals, Stream

from .utils import read_full_refresh

NUMBER_OF_PROPERTIES = 2000

logger = logging.getLogger("test_client")


@pytest.fixture(autouse=True)
def time_sleep_mock(mocker):
    time_mock = mocker.patch("time.sleep", lambda x: None)
    yield time_mock


def test_check_connection_ok(requests_mock, config):
    responses = [
        {"json": [], "status_code": 200},
    ]

    requests_mock.register_uri("GET", "/properties/v2/contact/properties", responses)
    ok, error_msg = SourceHubspotHistoryDeals().check_connection(logger, config=config)

    assert ok
    assert not error_msg


def test_check_connection_empty_config(config):
    config = {}

    with pytest.raises(KeyError):
        SourceHubspotHistoryDeals().check_connection(logger, config=config)


def test_check_connection_invalid_config(config):
    config.pop("start_date")

    with pytest.raises(KeyError):
        SourceHubspotHistoryDeals().check_connection(logger, config=config)


def test_check_connection_exception(config):
    ok, error_msg = SourceHubspotHistoryDeals().check_connection(logger, config=config)

    assert not ok
    assert error_msg


def test_check_connection_invalid_start_date_exception(config_invalid_date):
    with pytest.raises(InvalidStartDateConfigError):
        ok, error_msg = SourceHubspotHistoryDeals().check_connection(logger, config=config_invalid_date)
        assert not ok
        assert error_msg


@mock.patch("source_hubspot_history_deals.source.SourceHubspotHistoryDeals.get_custom_object_streams")
def test_streams(requests_mock, config):

    streams = SourceHubspotHistoryDeals().streams(config)

    assert len(streams) == 28


def test_check_credential_title_exception(config):
    config["credentials"].pop("credentials_title")

    with pytest.raises(Exception):
        SourceHubspotHistoryDeals().check_connection(logger, config=config)


def test_parse_and_handle_errors(some_credentials):
    response = MagicMock()
    response.status_code = HTTPStatus.TOO_MANY_REQUESTS

    with pytest.raises(HubspotRateLimited):
        API(some_credentials)._parse_and_handle_errors(response)


def test_convert_datetime_to_string():
    pendulum_time = pendulum.now()

    assert Stream._convert_datetime_to_string(pendulum_time, declared_format="date")
    assert Stream._convert_datetime_to_string(pendulum_time, declared_format="date-time")


def test_check_connection_backoff_on_limit_reached(requests_mock, config):
    """Error once, check that we retry and not fail"""
    responses = [
        {"json": {"error": "limit reached"}, "status_code": 429, "headers": {"Retry-After": "0"}},
        {"json": [], "status_code": 200},
    ]

    requests_mock.register_uri("GET", "/properties/v2/contact/properties", responses)
    source = SourceHubspotHistoryDeals()
    alive, error = source.check_connection(logger=logger, config=config)

    assert alive
    assert not error


def test_check_connection_backoff_on_server_error(requests_mock, config):
    """Error once, check that we retry and not fail"""
    responses = [
        {"json": {"error": "something bad"}, "status_code": 500},
        {"json": [], "status_code": 200},
    ]
    requests_mock.register_uri("GET", "/properties/v2/contact/properties", responses)
    source = SourceHubspotHistoryDeals()
    alive, error = source.check_connection(logger=logger, config=config)

    assert alive
    assert not error


class TestSplittingPropertiesFunctionality:
    BASE_OBJECT_BODY = {
        "createdAt": "2020-12-10T07:58:09.554Z",
        "updatedAt": "2021-07-31T08:18:58.954Z",
        "archived": False,
    }

    @staticmethod
    def set_mock_properties(requests_mock, url, fake_properties_list):
        properties_response = [
            {
                "json": [
                    {"name": property_name, "type": "string", "updatedAt": 1571085954360, "createdAt": 1565059306048}
                    for property_name in fake_properties_list
                ],
                "status_code": 200,
            },
        ]
        requests_mock.register_uri("GET", url, properties_response)

    # Mock the getter method that handles requests.
    def get(self, url, api, params=None):
        response = api._session.get(api.BASE_URL + url, params=params)
        return api._parse_and_handle_errors(response)



    def test_stream_with_splitting_properties_with_new_record(self, requests_mock, common_params, api, fake_properties_list):
        """
        Check working stream `workflows` with large list of properties using new functionality with splitting properties
        """

        parsed_properties = list(APIv3Property(fake_properties_list).split())
        self.set_mock_properties(requests_mock, "/properties/v2/deal/properties", fake_properties_list)

        test_stream = Deals(**common_params)

        ids_list = ["6043593519", "1092593519", "1092593518", "1092593517", "1092593516"]
        for property_slice in parsed_properties:
            record_responses = [
                {
                    "json": {
                        "results": [
                            {**self.BASE_OBJECT_BODY, **{"id": id, "properties": {p: "fake_data" for p in property_slice.properties}}}
                            for id in ids_list
                        ],
                        "paging": {},
                    },
                    "status_code": 200,
                }
            ]
            test_stream._sync_mode = SyncMode.full_refresh
            prop_key, prop_val = next(iter(property_slice.as_url_param().items()))
            requests_mock.register_uri("GET", f"{test_stream.url}?{prop_key}={prop_val}", record_responses)
            test_stream._sync_mode = None
            ids_list.append("1092593513")

        stream_records = read_full_refresh(test_stream)

        assert len(stream_records) == 6


@pytest.fixture(name="configured_catalog")
def configured_catalog_fixture():
    configured_catalog = {
        "streams": [
            {
                "stream": {
                    "name": "quotes",
                    "json_schema": {},
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["updatedAt"],
                },
                "sync_mode": "incremental",
                "cursor_field": ["updatedAt"],
                "destination_sync_mode": "append",
            }
        ]
    }
    return ConfiguredAirbyteCatalog.parse_obj(configured_catalog)


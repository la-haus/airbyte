#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pendulum
import pytest
from airbyte_cdk.models import SyncMode
from source_hubspot_history_deals.streams import (
    Contacts,
    Deals,
)

from .utils import read_full_refresh, read_incremental


@pytest.fixture(autouse=True)
def time_sleep_mock(mocker):
    time_mock = mocker.patch("time.sleep", lambda x: None)
    yield time_mock




@pytest.mark.parametrize(
    "stream, endpoint, cursor_value",
    [

        (Contacts, "contact", {"updatedAt": "2023-07-05T16:43:11Z"}),
        (Deals, "deal", {"updatedAt": "2023-07-05T16:43:11Z"}),
    ],
)
def test_streams_read(stream, endpoint, cursor_value, requests_mock, common_params, fake_properties_list):
    stream = stream(**common_params)
    responses = [
        {
            "json": {
                stream.data_field: [
                    {
                        "id": "test_id",
                        "created": "2023-07-05T16:43:11Z",
                    }
                    | cursor_value
                ],
            }
        }
    ]
    properties_response = [
        {
            "json": [
                {"name": property_name, "type": "string", "updatedAt": 1571085954360, "createdAt": 1565059306048}
                for property_name in fake_properties_list
            ],
            "status_code": 200,
        }
    ]


    requests_mock.register_uri("GET", stream_url, responses)
    requests_mock.register_uri("GET", "/marketing/v3/forms", responses)
    requests_mock.register_uri("GET", "/email/public/v1/campaigns/test_id", responses)
    requests_mock.register_uri("GET", f"/properties/v2/{endpoint}/properties", properties_response)

    records = read_full_refresh(stream)
    assert records


@pytest.mark.parametrize(
    "error_response",
    [
        {"json": {}, "status_code": 429},
        {"json": {}, "status_code": 502},
        {"json": {}, "status_code": 504},
    ],
)
def test_common_error_retry(error_response, requests_mock, common_params, fake_properties_list):
    """Error once, check that we retry and not fail"""
    properties_response = [
        {"name": property_name, "type": "string", "updatedAt": 1571085954360, "createdAt": 1565059306048}
        for property_name in fake_properties_list
    ]
    responses = [
        error_response,
        {
            "json": properties_response,
            "status_code": 200,
        },
    ]



@pytest.fixture(name="expected_custom_object_json_schema")
def expected_custom_object_json_schema():
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": ["null", "object"],
        "additionalProperties": True,
        "properties": {
            "id": {"type": ["null", "string"]},
            "createdAt": {"type": ["null", "string"], "format": "date-time"},
            "updatedAt": {"type": ["null", "string"], "format": "date-time"},
            "archived": {"type": ["null", "boolean"]},
            "properties": {"type": ["null", "object"], "properties": {"name": {"type": ["null", "string"]}}},
        },
    }




def test_get_custom_objects_metadata_success(requests_mock, expected_custom_object_json_schema, api):
    for (entity, fully_qualified_name, schema) in api.get_custom_objects_metadata():
        assert entity == "animals"
        assert fully_qualified_name == "p19936848_Animal"
        assert schema == expected_custom_object_json_schema

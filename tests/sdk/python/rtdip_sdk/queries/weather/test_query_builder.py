# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from src.sdk.python.rtdip_sdk.queries.weather import QueryBuilder
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.authentication.azure import DefaultAuth
from pytest_mock import MockerFixture

MOCK_TABLE = "mock_catalog.mock_scema.mock_table"
MOCK_CONNECTION = "mock_connection"


def _query_builder(parameters_dict: dict, query_type: str) -> str:
    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(
        dict.fromkeys(parameters_dict["tag_names"])
    )  # remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()

    parameters_dict = _parse_dates(parameters_dict)

    if query_type == "latest_point":
        return _latest_query_point(parameters_dict)

    if query_type == "latest_grid":
        return _latest_query_grid(parameters_dict)

    if query_type == "raw_point":
        return _raw_query_point(parameters_dict)
    
    if query_type == "raw_grif":
        return _raw_query_grid(parameters_dict)

   


def test_query_builder_raw_point(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.raw.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .raw(
            tagname_filter=["mock_tag"], start_date="2021-01-01", end_date="2021-01-02"
        )
    )
    assert data == {"test": "data"}


def test_query_builder_resample(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.resample.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE)
        .resample(
            tagname_filter=["mock_tag"],
            start_date="2021-01-01",
            end_date="2021-01-02",
            time_interval_rate="1",
            time_interval_unit="hour",
            agg_method="avg",
        )
    )
    assert data == {"test": "data"}


def test_query_builder_interpolate(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.interpolate.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source("mock_catalog.mock_scema.mock_table", status_column=None)
        .interpolate(
            tagname_filter=["mock_tag"],
            start_date="2021-01-01",
            end_date="2021-01-02",
            time_interval_rate="1",
            time_interval_unit="hour",
            agg_method="avg",
            interpolation_method="linear",
        )
    )
    assert data == {"test": "data"}


def test_query_builder_interpolation_at_time(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.interpolation_at_time.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .interpolation_at_time(
            tagname_filter=["mock_tag"],
            timestamp_filter=["2021-01-02T17:30:00+00:00", "2021-01-02T18:30:00+00:00"],
        )
    )
    assert data == {"test": "data"}


def test_query_builder_twa(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.time_weighted_average.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .time_weighted_average(
            tagname_filter=["mock_tag"],
            start_date="2021-01-01",
            end_date="2021-01-02",
            time_interval_rate="1",
            time_interval_unit="hour",
            step="metadata",
            source_metadata="mock_catalog.mock_schema.mock_table_metadata",
        )
    )
    assert data == {"test": "data"}


def test_query_builder_metadata(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.metadata.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE)
        .metadata(tagname_filter=["mock_tag"])
    )
    assert data == {"test": "data"}


def test_query_builder_latest(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.latest.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE)
        .latest(tagname_filter=["mock_tag"])
    )
    assert data == {"test": "data"}


def test_query_builder_circular_average(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.circular_average.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .circular_average(
            tagname_filter=["mock_tag"],
            start_date="2021-01-01",
            end_date="2021-01-02",
            time_interval_rate="1",
            time_interval_unit="hour",
            lower_bound=1,
            upper_bound=2,
        )
    )
    assert data == {"test": "data"}


def test_query_builder_circular_standard_deviation(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.circular_standard_deviation.get",
        return_value={"test": "data"},
    )

    data = (
        QueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .circular_standard_deviation(
            tagname_filter=["mock_tag"],
            start_date="2021-01-01",
            end_date="2021-01-02",
            time_interval_rate="1",
            time_interval_unit="hour",
            lower_bound=1,
            upper_bound=2,
        )
    )
    assert data == {"test": "data"}

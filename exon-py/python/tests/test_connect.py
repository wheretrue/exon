"""Tests for the exonpy module."""

# Copyright 2023 WHERE TRUE Technologies.
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

from unittest import mock

import exonpy


def test_connect(monkeypatch):
    # Create a connection to the Exome server.
    # We'll mock the _authenticate method so we don't need to worry about
    # credentials using pytest

    auth_mock = mock.Mock()
    auth_mock.return_value = "token"

    flight_connect_mock = mock.Mock()

    monkeypatch.setattr(exonpy, "_authenticate", auth_mock)
    monkeypatch.setattr(exonpy, "_flight_sql_connect", flight_connect_mock)

    with exonpy.connect("username", "password") as conn:
        assert isinstance(conn, exonpy.ExomeConnection)
        assert flight_connect_mock.call_count == 1
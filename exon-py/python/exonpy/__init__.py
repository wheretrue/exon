"""ExonPy is a Python library for working with exon data."""

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

import os
from contextlib import contextmanager
from adbc_driver_flightsql import DatabaseOptions
import adbc_driver_flightsql.dbapi as flight_sql
import boto3
import botocore


class ExomeError(Exception):
    """An error raised by Exome."""

    def __init__(self, *args: object) -> None:
        """Initialize the error."""
        super().__init__(*args)


class ExomeConnection:
    """A connection to an Exome server."""

    def __init__(self, conn: flight_sql.Connection):
        """Create a new connection to an Exome server."""
        self.conn = conn

    @contextmanager
    def cursor(self):
        """Return a cursor for the connection."""
        cursor = self.conn.cursor()

        try:
            yield cursor

        # pylint: disable=invalid-name
        except ExomeError as e:
            raise e

        finally:
            cursor.close()

    def close(self):
        """Close the connection."""
        self.conn.close()

    def __repr__(self):
        """Return a string representation of the connection."""
        return "ExomeConnection()"


def _authenticate(username: str, password: str):
    """Authenticate the user."""
    client = boto3.client("cognito-idp", region_name="us-west-2")
    client_id = os.environ["EXOME_AUTH_CLIENT_ID"]

    try:
        auth_result = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": username,
                "PASSWORD": password,
            },
            ClientId=client_id,
        )

        id_token = auth_result["AuthenticationResult"]["IdToken"]
        return id_token

    # pylint: disable=invalid-name
    except botocore.exceptions.ClientError as e:
        raise ExomeError("Authentication failed") from e


def _flight_sql_connect(uri: str, skip_verify: bool, token: str):
    """Connect to an Exome server."""
    try:
        flight_connection = flight_sql.connect(
            uri=uri,
            db_kwargs={
                DatabaseOptions.TLS_SKIP_VERIFY.value: skip_verify,
                DatabaseOptions.AUTHORIZATION_HEADER.value: token,
            },
        )

    # pylint: disable=invalid-name
    except Exception as e:
        raise ExomeError("Connection failed") from e

    return flight_connection


# Connect should be able to be a context manager
@contextmanager
def connect(username: str, password: str, **kwargs):
    """Connect to an Exome server."""
    token = _authenticate(username, password)

    uri = kwargs.get("uri", "grpc://localhost:50051")
    skip_verify = kwargs.get("skip_verify", True)

    flight_connection = _flight_sql_connect(uri, skip_verify, token)

    exome_conn = ExomeConnection(flight_connection)

    try:
        yield exome_conn

    # pylint: disable=invalid-name
    except ExomeError as e:
        raise e

    finally:
        exome_conn.close()

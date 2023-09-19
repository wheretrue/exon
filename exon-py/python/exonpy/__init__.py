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

from contextlib import contextmanager

import adbc_driver_flightsql.dbapi as flight_sql
import grpc
from adbc_driver_flightsql import DatabaseOptions

import exonpy.proto.exome.v1.catalog_pb2
import exonpy.proto.exome.v1.catalog_pb2_grpc


class ExomeError(Exception):
    """An error raised by Exome."""

    def __init__(self, *args: object) -> None:
        """Initialize the error."""
        super().__init__(*args)


class ExomeGrpcConnection:
    """A connection to an Exome server."""

    def __init__(
        self,
        stub: exonpy.proto.exome.v1.catalog_pb2_grpc.CatalogServiceStub,
        token: str,
    ):
        """Create a new connection to an Exome server."""
        self.stub = stub
        self.token = token


class ExomeConnection:
    """A connection to an Exome server."""

    def __init__(
        self,
        conn: flight_sql.Connection,
        exome_grpc_connection: ExomeGrpcConnection,
        organization_name: str = "Public",
    ):
        """Create a new connection to an Exome server."""
        self.conn = conn
        self.exome_grpc_connection = exome_grpc_connection
        self.organization_name = organization_name

    def set_library_id(self, library_id: str):
        """Set the library ID."""
        # Make api call to associated the library_id with the connection

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


def _flight_sql_connect(
    uri: str, skip_verify: bool, token: str, protocol: str = "grpc"
):
    """Connect to an Exome server."""
    try:
        flight_connection = flight_sql.connect(
            uri=f"{protocol}://{uri}",
            db_kwargs={
                DatabaseOptions.TLS_SKIP_VERIFY.value: str(skip_verify).lower(),
                DatabaseOptions.AUTHORIZATION_HEADER.value: token,
            },
        )

    # pylint: disable=invalid-name
    except Exception as e:
        raise ExomeError("Connection failed") from e

    return flight_connection


def _connect_to_exome_request(
    stub: exonpy.proto.exome.v1.catalog_pb2_grpc.CatalogServiceStub,
    get_token_request: exonpy.proto.exome.v1.catalog_pb2.GetTokenRequest,
) -> str:
    """Make the actual request to get the token."""

    try:
        token_response = stub.GetToken(get_token_request)

    # pylint: disable=invalid-name
    except grpc.RpcError as e:
        raise ExomeError("Authentication failed") from e

    # pylint: disable=invalid-name
    except Exception as e:
        raise ExomeError("Connection failed") from e

    token = token_response.token

    return token


def connect_to_exome(uri: str, username: str, password: str) -> ExomeGrpcConnection:
    channel = grpc.insecure_channel(uri)
    stub = exonpy.proto.exome.v1.catalog_pb2_grpc.CatalogServiceStub(channel)

    token_request = exonpy.proto.exome.v1.catalog_pb2.GetTokenRequest(
        email=username, password=password
    )

    token = _connect_to_exome_request(stub, token_request)
    exome_grpc_connection = ExomeGrpcConnection(stub, token)

    return exome_grpc_connection


# Connect should be able to be a context manager
@contextmanager
def connect(username: str, password: str, organization_name: str = "Public", **kwargs):
    """Connect to an Exome server."""
    uri = kwargs.get("uri", "localhost:50051")

    exome_connection = connect_to_exome(uri, username, password)

    skip_verify = kwargs.get("skip_verify", True)

    flight_connection = _flight_sql_connect(uri, skip_verify, exome_connection.token)

    exome_conn = ExomeConnection(flight_connection, exome_connection, organization_name)

    try:
        yield exome_conn

    # pylint: disable=invalid-name
    except ExomeError as e:
        raise e

    finally:
        exome_conn.close()

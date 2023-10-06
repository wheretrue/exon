"""The Exome Python client library."""
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

import logging
import warnings
from contextlib import contextmanager

import adbc_driver_flightsql.dbapi as flight_sql
import grpc
from adbc_driver_flightsql import DatabaseOptions

from exon_py.proto.exome.v1 import catalog_pb2, catalog_pb2_grpc, health_check_pb2, health_check_pb2_grpc

warnings.simplefilter(action="ignore", category=Warning)

logger = logging.getLogger(__name__)

__all__ = [
    "connect",
    "ExomeConnection",
]


class ExomeError(Exception):
    """An error raised by Exome."""

    def __init__(self, *args: object) -> None:
        """Initialize the error."""
        super().__init__(*args)

    @classmethod
    def from_grpc_error(cls, grpc_error: grpc.RpcError):
        """Create an ExomeError from a gRPC error."""
        return cls(grpc_error.details())


class ExomeGrpcConnection:
    """A connection to an Exome server."""

    def __init__(
        self,
        stub: catalog_pb2_grpc.CatalogServiceStub,
        token: str,
    ):
        """Create a new connection to an Exome server."""
        self.stub = stub
        self.token = token


class ExomeConnection:
    """A connection to an Exome server."""

    def __init__(
        self,
        flight_conn: flight_sql.Connection,
        exome_grpc_connection: ExomeGrpcConnection,
    ):
        """Create a new connection to an Exome server."""
        self.flight_conn = flight_conn
        self.exome_grpc_connection = exome_grpc_connection

    def set_library_id(self, library_id: str):
        """Set the library ID."""
        # Make api call to associated the library_id with the connection

    @contextmanager
    def cursor(self):
        """Return a cursor for the connection."""
        cursor = self.flight_conn.cursor()

        try:
            yield cursor

        # pylint: disable=invalid-name
        except ExomeError as e:
            raise e

        finally:
            cursor.close()

    def close(self):
        """Close the connection."""
        self.flight_conn.close()

    def __repr__(self):
        """Return a string representation of the connection."""
        return "ExomeConnection()"


def _flight_sql_connect(uri: str, token: str, protocol: str = "grpc+tls", *, skip_verify: bool = True):
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
        error_msg = f"Error connecting to Exome server: {e}"
        raise ExomeError(error_msg) from None

    return flight_connection


def _connect_to_exome_request(
    stub: catalog_pb2_grpc.CatalogServiceStub,
    get_token_request: catalog_pb2.GetTokenRequest,
) -> str:
    """Make the actual request to get the token."""
    try:
        token_response = stub.GetToken(get_token_request)

    # pylint: disable=invalid-name
    except grpc.RpcError as e:
        logger.error("Error connecting to Exome server: %s", e)
        raise ExomeError.from_grpc_error(e)

    # pylint: disable=invalid-name
    except Exception as e:
        raise ExomeError(str(e)) from None

    token = token_response.token

    return token


def connect_to_exome(uri: str, username: str, password: str) -> ExomeGrpcConnection:
    creds = grpc.ssl_channel_credentials(root_certificates=None, private_key=None, certificate_chain=None)

    channel = grpc.secure_channel(uri, creds)
    stub = catalog_pb2_grpc.CatalogServiceStub(channel)

    token_request = catalog_pb2.GetTokenRequest(email=username, password=password)

    token = _connect_to_exome_request(stub, token_request)
    exome_grpc_connection = ExomeGrpcConnection(stub, token)

    logger.info("Connected to Exome server at %s", uri)

    return exome_grpc_connection


@contextmanager
def connect(
    username: str,
    password: str,
    organization_name: str = "Public",  # TODO: unused for now
    *,
    uri: str = "adbc.exome.wheretrue.com:443",
    skip_verify: bool = False,
):
    """Connect to an Exome server."""
    exome_connection = connect_to_exome(uri, username, password)

    flight_connection = _flight_sql_connect(uri, exome_connection.token, skip_verify=skip_verify)

    exome_conn = ExomeConnection(flight_connection, exome_connection)

    try:
        yield exome_conn

    # pylint: disable=invalid-name
    except ExomeError as e:
        raise e

    finally:
        exome_conn.close()


def health_check(uri: str) -> health_check_pb2.HealthCheckResponse:
    """Return the health of the Exome server.

    Args:
        uri (str): The URI of the Exome server. For example `adbc.exome.wheretrue.com`.

    Returns:
        health_check_pb2.HealthCheckResponse: The health of the Exome server.
    """
    creds = grpc.ssl_channel_credentials(root_certificates=None, private_key=None, certificate_chain=None)

    channel = grpc.secure_channel(uri, creds)

    stub = health_check_pb2_grpc.HealthStub(channel)

    return stub.Check(health_check_pb2.HealthCheckRequest(service="exome"))

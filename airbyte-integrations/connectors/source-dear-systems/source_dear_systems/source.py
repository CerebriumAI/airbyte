#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class DearSystemsStream(HttpStream, ABC):
    """

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class DearSystemsStream(HttpStream, ABC)` which is the current class
    `class Customers(DearSystemsStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(DearSystemsStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalDearSystemsStream((DearSystemsStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    url_base = "https://inventory.dearsystems.com/ExternalApi/"

    def request_headers(self, *args, **kwargs):
        for key, value in kwargs.items():
            print("{0} = {1}".format(key, value))

        for arg in args:
            print("another arg through *argv:", arg)


        # return {"api-auth-applicationkey": self.config.get("api_key"), "api-auth-accountid": self.config.get("account_id")}
        # return {"api-auth-applicationkey": "e4b6f3ec-e89a-6fdb-9275-2b3ccecae968", "api-auth-accountid": "f5b894f2-7070-4ecf-82ec-98bc8f5411c3"}
        return {}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        self.logger.info(f"Response {response}")

        json_response = response.json()
        for record in json_response:
            yield record


def path() -> str:
    return "v2/ref/productavailability?Page=1&Limit=100"


class ProductAvailability(DearSystemsStream):
    primary_key = "ID"


# Source
class SourceDearSystems(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        return [ProductAvailability()]

#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth


class DearBase(HttpStream):
    url_base = "https://inventory.dearsystems.com/ExternalApi/"

    def __init__(self, account_id: str, api_key: str, **kwargs):
        super().__init__()
        self.account_id = account_id
        self.api_key = api_key

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        current_page = response.json()['Page']
        total_items = response.json()['Total']

        next_page = current_page + 1

        total_pages = int(total_items / 100) + 1

        if total_pages == current_page:
            return None

        return {'Page': next_page}

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return next_page_token

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        api_key = self.api_key
        account_id = self.account_id

        return {"api-auth-applicationkey": api_key, "api-auth-accountid": account_id}


class ProductAvailability(DearBase):
    primary_key = "ID"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/ref/productavailability?Limit=100"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("ProductAvailabilityList", []):
            yield record


class Sales(DearBase):
    primary_key = "ID"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "v2/saleList?Limit=100"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("SaleList", []):
            yield record


class SourceDearInventory(AbstractSource):

    def __init__(self):
        self.config = None

    def check_connection(self, logger, config) -> Tuple[bool, any]:

        if not config['account_id']:
            return False, 'Account ID is required'

        if not config['api_key']:
            return False, 'API Key is required'

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()

        return [ProductAvailability(account_id=config['account_id'], api_key=config['api_key'], auth=auth),
                Sales(account_id=config['account_id'], api_key=config['api_key'], auth=auth), ]

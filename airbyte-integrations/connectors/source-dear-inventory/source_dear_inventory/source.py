#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth


class DearBase(HttpStream):
    url_base = "https://inventory.dearsystems.com/ExternalApi/"

    def __init__(self, config: Mapping[str, str], **kwargs):
        super().__init__()
        self.account_id = config['account_id']
        self.api_key = config['api_key']
        self.created_since = config['created_since']

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
    ) -> Optional[Mapping[str, Any]]:
        return next_page_token

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        api_key = self.api_key
        account_id = self.account_id

        return {"api-auth-applicationkey": api_key, "api-auth-accountid": account_id}


class DearSubStream(HttpSubStream):
    def next_page_token(self, response: requests.Response):
        return None

    def stream_slices(
            self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        parent_stream_slices = self.parent.stream_slices(
            sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_state=stream_state
        )
        # iterate over all parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                yield {"parent": record, "sub_parent": stream_slice}


class ProductAvailability(DearBase):
    primary_key = "ID"

    def path(self, **kwargs) -> str:
        return "v2/ref/productavailability?Limit=500"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("ProductAvailabilityList", []):
            yield record


class Location(DearBase):
    primary_key = "ID"

    def path(self, **kwargs) -> str:
        return "v2/ref/location"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("LocationList", []):
            yield record


class Sale(DearBase):
    primary_key = "SaleID"

    def path(self, **kwargs) -> str:
        return "v2/saleList?Limit=500" + f"&createdSince={self.created_since}" if self.created_since else ''

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("SaleList", []):
            yield record


class SalesInvoice(DearSubStream, DearBase):
    primary_key = "SaleID"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"v2/sale/invoice?SaleID={stream_slice['parent']['SaleID']}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("Invoices", []):
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

        return [
            Location(config=config, auth=auth),
            ProductAvailability(config=config, auth=auth),
            Sale(config=config, auth=auth),
            SalesInvoice(Sale(config=config, auth=auth), config=config, auth=auth)
        ]

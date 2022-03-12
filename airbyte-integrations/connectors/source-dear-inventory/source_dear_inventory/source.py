#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

ITEMS_PER_PAGE = 500


class DearBase(HttpStream):
    url_base = "https://inventory.dearsystems.com/ExternalApi/"

    def __init__(self, config: Mapping[str, str], **kwargs):
        super().__init__()
        self.account_id = config['account_id']
        self.api_key = config['api_key']

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code != 200:
            return 60

        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        current_page = response.json()['Page']
        total_items = response.json()['Total']

        next_page = current_page + 1

        total_pages = int(total_items / ITEMS_PER_PAGE) + 1

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
            sync_mode=SyncMode.incremental, cursor_field=cursor_field, stream_state=stream_state
        )

        # iterate over all parent stream_slices
        for stream_slice in parent_stream_slices:
            parent_records = self.parent.read_records(sync_mode=SyncMode.incremental, stream_slice=stream_slice)

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                yield {"parent": record, "sub_parent": stream_slice}


class ProductAvailability(DearBase):
    primary_key = "ID"

    def path(self, **kwargs) -> str:
        return f"v2/ref/productavailability?Limit={ITEMS_PER_PAGE}"

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


class Sale(DearBase, IncrementalMixin):
    primary_key = "SaleID"
    cursor_field = "Updated"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = "2000-01-01T00:00:00Z"

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: ""}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]

    def path(self, **kwargs) -> str:

        updated = f"&UpdatedSince={self.state[self.cursor_field]}" if self.state[self.cursor_field] else ''

        path = f"v2/saleList?Limit={ITEMS_PER_PAGE}" + updated
        print('Sale Path: ', path)
        return path

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()

        for record in json_response.get("SaleList", []):
            yield record

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = record[self.cursor_field]
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record


class SaleInvoice(DearSubStream, DearBase):
    # Don't throw an error if request fails - sometimes returns 400
    raise_on_http_errors = False
    primary_key = "SaleID"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"v2/sale/invoice?SaleID={stream_slice['parent']['SaleID']}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code != 200:
            yield {}
        else:
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

        sale = Sale(config=config, auth=auth)

        return [
            Location(config=config, auth=auth),
            ProductAvailability(config=config, auth=auth),
            sale,
            SaleInvoice(sale, config=config, auth=auth)
        ]

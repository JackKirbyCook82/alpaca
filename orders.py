# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

import multiprocessing
import pandas as pd
from parse import parse
from datetime import date as Date

from finance.enumerations import Instrument, Position, Status, Tenure, Terms, Intent
from finance.logging import Logging
from finance.osi import OSI
from support.custom import ReversibleDict as RDict
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webpayloads import WebPayload
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader", "AlpacaHeaders"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


tenure_mapping = RDict({Tenure.DAY: "day", Tenure.GTC: "gtc", Tenure.FOK: "fok"})
term_mapping = RDict({Terms.MARKET: "market", Terms.LIMIT: "limit", Terms.STOP: "stop"})
position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
intent_mapping = RDict({Intent.OPEN: "open", Intent.CLOSE: "close"})

intent_formatter = lambda position, intent: f"{position[position, False]}_to_{intent_mapping[intent, False]}"
position_formatter = lambda position: position_mapping[position, False]
tenure_formatter = lambda tenure: tenure_mapping[tenure, False]
term_formatter = lambda term: intent_mapping[term, False]
quantity_formatter = lambda quantity: f"{quantity:.0f}"
price_formatter = lambda price: f"{price:.2f}"

timestamp_parser = lambda string: pd.to_datetime(string)
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike
intent_parser = lambda string: intent_mapping[parse("{position}_to_{intent}", string)["intent"], True]
position_parser = lambda string: position_mapping[string, True]
tenure_parser = lambda string: tenure_mapping[string, True]
term_parser = lambda string: term_mapping[string, True]
quantity_parser = lambda string: abs(int(string))


class AlpacaHeaders:
    Uploading = {"order": str, "asset": str, "spread": int, "ticker": str, "expire": Date, "option": int, "strike": float, "position": int, "quantity": int}
    Downloading = {}


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaUploadingOrder(WebURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
class AlpacaDownloadingOrder(WebURL, parameters={"status": "all", "nested": True}, headers={"accept": "application/json"}):
    @staticmethod
    def path(*args, order, **kwargs): return [str(order)]


class AlpacaOrderPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "qty": "1"}, multiple=False, optional=False):
    class Price(WebPayload.Value, key="price", locator="limit_price", parser=price_formatter): pass
    class Tenure(WebPayload.Value, key="tenure", locator="time_in_force", parser=tenure_formatter): pass
    class Terms(WebPayload.Value, key="term", locator="type", parser=term_formatter): pass
    class Securities(WebPayload.Mapping, key="securities", locator="legs", multiple=True, optional=False):
        class Osi(WebPayload.Value, key="osi", locator="symbol"): pass
        class Intent(WebPayload.Value, key="intent", locator="position_intent", parser=intent_formatter): pass
        class Position(WebPayload.Value, key="position", locator="side", parser=position_formatter): pass
        class Quantity(WebPayload.Value, key="quantity", locator="ratio_qty", parser=quantity_formatter): pass


class AlpacaOrderData(WebJSON.Mapping, multiple=False, optional=False):
    class Order(WebJSON.Text, key="order", locator="id", parser=str): pass
    class Created(WebJSON.Text, key="created", locator="created_at", parser=timestamp_parser): pass
    class Submitted(WebJSON.Text, key="submitted", locator="submitted_at", parser=timestamp_parser): pass
    class Filled(WebJSON.Text, key="filled", locator="filled_at", parser=timestamp_parser): pass
    class Expired(WebJSON.Text, key="expired", locator="expired_at", parser=timestamp_parser): pass
    class Canceled(WebJSON.Text, key="canceled", locator="canceled_at", parser=timestamp_parser): pass
    class Failed(WebJSON.Text, key="failed", locator="failed_at", parser=timestamp_parser): pass
    class Status(WebJSON.Text, key="status", locator="status", parser=Status): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass
    class Securities(WebJSON.Mapping, key="securities", locator="legs", parser=dict, multiple=True, optional=False):
        class Asset(WebJSON.Text, key="asset", locator="asset_id", parser=str): pass
        class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
        class Expire(WebJSON.Text, key="expire", locator="expire", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="option", parser=option_parser): pass
        class Strike(WebJSON.Text, key="strike", locator="strike", parser=strike_parser): pass
        class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
        class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=quantity_parser): pass


class AlpacaUploadingOrderPage(WebJSONPage):
    def execute(self, *args, acquisition, tenure, term, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        url = AlpacaUploadingOrder(**parameters)
        securities = [{"osi": record.osi, "position": record.position, "intent": (record.postion, acquisition.intent), "quantity": record.quantity} for record in acquisition.records]
        payload = AlpacaOrderData({"price": acquisition.price, "tenure": tenure, "term": term, "securities": securities})
        json = self.load(url, payload=payload)
        data = AlpacaOrderData(json, *args, **kwargs)
        mapping = data(*args, **kwargs)
        records = mapping.pop("securities")
        records = [mapping | record for record in records]
        order = pd.DataFrame.from_records(records)
        order["expire"] = pd.to_datetime(order["expire"])
        order["strike"] = pd.to_numeric(order["strike"])
        order["spread"] = acquisition.spread
        columns = list(AlpacaHeaders.Uploading.keys())
        return order[columns]


# class AlpacaDownloadingOrderPage(AlpacaOrderPage):
#     def execute(self, *args, order, **kwargs):
#         parameters = dict(authenticator=self.authenticator)
#         url = AlpacaDownloadingOrder(order=order, **parameters)
#         json = self.load(url)
#         data = AlpacaOrderData(json, *args, **kwargs)
#         return data


class AlpacaOrderUploader(WebStream, Logging, page=AlpacaUploadingOrderPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()

    def __call__(self, acquisitions, /, **kwargs):
        assert isinstance(acquisitions, list)
        columns = list(AlpacaHeaders.Uploading.keys())
        if not bool(acquisitions): return pd.DataFrame(columns=columns)
        acquisitions = self.filter(acquisitions, **kwargs)
        acquisitions = list(acquisitions)
        if not bool(acquisitions): return pd.DataFrame(columns=columns)
        orders = self.uploader(acquisitions, **kwargs)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        self.results(orders, title="Uploaded", instrument=Instrument.OPTION)
        return orders

    def filter(self, acquisitions, /, **kwargs):
        for acquisition in acquisitions:
            if acquisition.signature in self.history: continue
            with self.mutex: self.history.add(acquisition.signature)
            yield acquisition

    def uploader(self, acquisitions, /, **kwargs):
        for acquisition in acquisitions:
            order = self.page(acquisition=acquisition, **kwargs)
            securities = [f"{str(record.osi)}={int(record.position) * int(record.quantity):.0f}" for record in acquisition.records]
            self.console("Uploaded", f"Acquisition[{', '.join(securities)}]")
            self.console("Uploaded", f"Acquisition[Tight={acquisition.tightness:.2f}, Money={acquisition.moneyness:.2f}, Active={acquisition.activity:.2f}]")
            yield order

    @property
    def history(self): return self.__history
    @property
    def mutex(self): return self.__mutex


# class AlpacaOrderDownloader(WebStream, Logging, page=AlpacaDownloadingOrderPage):
#     def __call__(self, orders, **kwargs):
#         assert isinstance(orders, (list, str))
#         assert all([isinstance(order, str) for order in orders]) if isinstance(orders, list) else True
#         if isinstance(orders, str): orders = [orders]
#         if not bool(orders): return pd.DataFrame(columns=AlpacaOrder)
#         holdings = self.downloader(orders, **kwargs)
#         holdings = pd.concat(list(holdings), axis=0)
#         holdings = holdings.sort_values(by=["order", "asset"], inplace=False)
#         holdings = holdings.reset_index(drop=True, inplace=False)
#         self.results(holdings, title="Downloaded", instrument=Instrument.OPTION)
#         return holdings
#
#     def downloader(self, orders, /, **kwargs):
#         for order in set(orders):
#             holdings = self.page(order=order, **kwargs)
#             yield holdings




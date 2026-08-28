# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook
@file:   alpaca/orders.py

"""

import multiprocessing
import pandas as pd
from parse import parse
from pprint import pformat
from datetime import date as Date
from datetime import datetime as DateTime

from finance.enumerations import Instrument, Option, Position, Status, Tenure, Terms, Intent, Spread
from finance.logging import Logging
from finance.osi import OSI
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webpayloads import WebPayload
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.custom import ReversibleDict as RDict
from support.files import File, Header
from support.custom import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader", "AlpacaOrderFile"]
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

order_typing = {"order": str, "asset": str, "spread": int, "ticker": str, "expire": Date, "option": int, "strike": float, "position": int, "quantity": int}
order_formatting = {"spread": int, "expire": lambda expire: expire.strftime("%Y%m%d"), "option": int, "position": int}
order_parsing = {"spread": Spread, "expire": lambda string: DateTime.strptime(string, "%Y%m%d").date(), "option": Option, "position": Position}
order_columns = ["order", "asset", "spread", "ticker", "expire", "option", "strike", "position", "quantity"]
order_header = Header(order_columns, order_typing, order_formatting, order_parsing)


class AlpacaOrderFile(File, header=order_header):
    def results(self, orders, *args, title, **kwargs):
        tickers = "|".join(list(orders["ticker"].unique()))
        expires = DateRange(list(orders["expire"].unique()))
        expires = f"{expires.minimum.strftime('%Y%m%d')}->{expires.maximum.strftime('%Y%m%d')}"
        self.console(str(title), f"Orders[{str(tickers)}, {str(expires)}, {len(orders):.0f}]")


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


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


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, acquisition, tenure, term, dryrun=False, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        url = AlpacaOrderURL(**parameters)
        securities = [{"osi": record.osi, "position": record.position, "intent": (record.postion, acquisition.intent), "quantity": record.quantity} for record in acquisition.records]
        payload = AlpacaOrderData({"price": acquisition.price, "tenure": tenure, "term": term, "securities": securities})
        if bool(dryrun):
            print("\033[34m" + pformat(url) + "\033[0m")
            print("\033[34m" + pformat(payload) + "\033[0m")
            return None
        json = self.load(url, payload=payload)
        data = AlpacaOrderData(json, *args, **kwargs)
        mapping = data(*args, **kwargs)
        records = mapping.pop("securities")
        records = [mapping | record for record in records]
        order = pd.DataFrame.from_records(records)
        order["expire"] = pd.to_datetime(order["expire"])
        order["strike"] = pd.to_numeric(order["strike"])
        order["spread"] = acquisition.spread
        return order[order_columns]


class AlpacaOrderUploader(WebStream, Logging, page=AlpacaOrderPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()

    def __call__(self, acquisitions, /, **kwargs):
        assert isinstance(acquisitions, list)
        if not bool(acquisitions): return pd.DataFrame(columns=order_columns)
        acquisitions = self.filter(acquisitions, **kwargs)
        acquisitions = list(acquisitions)
        if not bool(acquisitions): return pd.DataFrame(columns=order_columns)
        orders = self.uploader(acquisitions, **kwargs)
        if orders is None: return pd.DataFrame(columns=order_columns)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        scope = self.scope(orders, instruments=Instrument.OPTION)
        self.results(scope=scope, size=len(orders.index), title="Uploaded")
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




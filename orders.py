# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook
@file:   alpaca/orders.py

"""

import math
import multiprocessing
import pandas as pd
from abc import ABC, abstractmethod
from datetime import date as Date
from datetime import datetime as Datetime

from alpaca.website import AlpacaDownloadURL, AlpacaUploadURL, AlpacaDownloadPage, AlpacaUploadPage, AlpacaDownloader
from finance.enumerations import Instrument, Option, Position, Status, Tenure, Terms, Intent, Action, Spread
from finance.osi import OSI
from webscraping.webpayloads import WebPayload
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.custom import ReversibleDict as RDict
from support.files import File, Header
from support.custom import DateRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader", "AlpacaOrderDownloader", "AlpacaOrderFile"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


status_mapping = RDict({Status.EXECUTING: "new", Status.PARTIAL: "partially_filled"})
tenure_mapping = RDict({Tenure.DAY: "day", Tenure.GTC: "gtc", Tenure.FOK: "fok"})
term_mapping = RDict({Terms.MARKET: "market", Terms.LIMIT: "limit", Terms.STOP: "stop"})

purpose_formatter = lambda purpose: f"{str(purpose.action).lower()}_to_{str(purpose.intent).lower()}"
tenure_formatter = lambda tenure: tenure_mapping[tenure, False]
term_formatter = lambda term: term_mapping[term, False]
action_formatter = lambda action: str(action).lower()
date_formatter = lambda date: date.strftime("%Y%m%d")
quantity_formatter = lambda quantity: f"{quantity:.0f}"
price_formatter = lambda price: f"{price:.2f}"

position_parser = lambda string: Position(math.prod(list(map(int, [function(str(value).upper()) for function, value in zip([Action, Intent], str(string).split("_to_"))]))))
status_parser = lambda string: status_mapping[string, True] if (string, True) in status_mapping else Status[str(string).upper()]
date_parser = lambda string: Datetime.strptime(string, "%Y%m%d").date()
tenure_parser = lambda string: tenure_mapping[string, True]
term_parser = lambda string: term_mapping[string, True]
quantity_parser = lambda string: abs(int(string))
timestamp_parser = lambda string: pd.to_datetime(string)
ticker_parser = lambda string: OSI(string).ticker
expire_parser = lambda string: OSI(string).expire
option_parser = lambda string: OSI(string).option
strike_parser = lambda string: OSI(string).strike

order_typing = {"date": Datetime, "order": str, "asset": str, "term": int, "tenure": int, "spread": int, "ticker": str, "expire": Date, "option": int, "strike": float, "position": int, "quantity": int}
order_formatting = {"date": date_formatter, "term": int, "tenure": int, "spread": int, "expire": date_formatter, "option": int, "position": int}
order_parsing = {"term": Terms, "tenure": Tenure, "spread": Spread, "expire": date_parser, "option": Option, "position": Position}
order_columns = ["order", "asset", "date", "term", "tenure", "spread", "ticker", "expire", "option", "strike", "position", "quantity"]
order_header = Header(order_columns, order_typing, order_formatting, order_parsing)


class AlpacaOrderFile(File, header=order_header):
    def results(self, orders, *args, title, **kwargs):
        tickers = "|".join(list(orders["ticker"].unique()))
        expires = DateRange(list(orders["expire"].unique()))
        expires = f"{expires.minimum.strftime('%Y%m%d')}->{expires.maximum.strftime('%Y%m%d')}"
        self.console(str(title), f"Orders[{str(tickers)}, {str(expires)}, {len(orders):.0f}]")


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]): pass
class AlpacaOrderUploadURL(AlpacaUploadURL, AlpacaOrderURL): pass
class AlpacaOrderDownloadURL(AlpacaDownloadURL, AlpacaOrderURL, parameters={"limit": 500, "nested": "true", "status": "all"}):
    def parameters(self, *args, **kwargs):
        tickers = self.tickers(*args, **kwargs)
        dates = self.dates(*args, **kwargs)
        return tickers | dates

    @staticmethod
    def dates(*args, dates, **kwargs): return {"after": dates.minimum.strftime("%Y-%m-%d"), "until": dates.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def tickers(*args, tickers, **kwargs): return {"symbols": ",".join(list(tickers))}


class AlpacaOrderUploadPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "qty": "1"}, multiple=False, optional=False):
    class Price(WebPayload.Value, key="price", locator="limit_price", parser=price_formatter): pass
    class Tenure(WebPayload.Value, key="tenure", locator="time_in_force", parser=tenure_formatter): pass
    class Terms(WebPayload.Value, key="term", locator="type", parser=term_formatter): pass
    class Securities(WebPayload.Mapping, key="securities", locator="legs", multiple=True, optional=False):
        class Osi(WebPayload.Value, key="osi", locator="symbol"): pass
        class Purpose(WebPayload.Value, key="purpose", locator="position_intent", parser=purpose_formatter): pass
        class Action(WebPayload.Value, key="action", locator="side", parser=action_formatter): pass
        class Quantity(WebPayload.Value, key="quantity", locator="ratio_qty", parser=quantity_formatter): pass


class AlpacaOrderData(WebJSON, multiple=False, optional=False):
    class Order(WebJSON.Text, key="order", locator="id", parser=str): pass
    class Date(WebJSON.Text, key="date", locator="created_at", parser=timestamp_parser): pass
    class Status(WebJSON.Text, key="status", locator="status", parser=status_parser): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass
    class Securities(WebJSON, key="securities", locator="legs", multiple=True, optional=False):
        class Asset(WebJSON.Text, key="asset", locator="asset_id", parser=str): pass
        class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
        class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
        class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
        class Position(WebJSON.Text, key="position", locator="position_intent", parser=position_parser): pass
        class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=quantity_parser): pass


class AlpacaOrderPage(WebJSONPage, ABC):
    def execute(self, json, *args, **kwargs):
        data = self.data(json, *args, **kwargs)
        mapping = data(*args, **kwargs)
        records = mapping.pop("securities")
        records = [mapping | record for record in records]
        if not records: return None
        orders = pd.DataFrame.from_records(records)
        orders["expire"] = pd.to_datetime(orders["expire"])
        orders["strike"] = pd.to_numeric(orders["strike"])
        return orders

    @abstractmethod
    def data(self, json, *args, **kwargs): pass


class AlpacaOrderUploadPage(AlpacaUploadPage, AlpacaOrderPage, url=AlpacaOrderUploadURL, data=AlpacaOrderData):
    def __call__(self, *args, target, tenure, term, **kwargs):
        url = self.url(*args, **kwargs)
        securities = [{"osi": record.osi, "purpose": record.purpose, "action": record.purpose.action, "quantity": record.quantity} for record in target]
        payload = AlpacaOrderUploadPayload({"price": target.price, "tenure": tenure, "term": term, "securities": securities})
        json = self.load(url, *args, payload=payload, **kwargs)
        orders = self.execute(json, *args, **kwargs)
        return orders


class AlpacaOrderDownloadPage(AlpacaDownloadPage, AlpacaOrderPage, url=AlpacaOrderDownloadURL, data=AlpacaOrderData):
    def __call__(self, *args, tickers, dates, **kwargs):
        url = self.url(*args, tickers=tickers, dates=dates, **kwargs)
        json = self.load(url, *args, payload=None, **kwargs)
        orders = self.execute(json, *args, **kwargs)
        return orders


class AlpacaOrderUploader(AlpacaDownloader, page=AlpacaOrderUploadPage, columns=order_columns, instrument=Instrument.OPTION):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()

    def __call__(self, targets, /, **kwargs):
        assert isinstance(targets, list)
        if not bool(targets): return pd.DataFrame(columns=self.columns)
        targets = list(self.filter(targets, **kwargs))
        if not bool(targets): return pd.DataFrame(columns=self.columns)
        orders = list(self.uploader(targets, **kwargs))
        if not bool(orders): return pd.DataFrame(columns=self.columns)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        scope = self.scope(orders)
        results = self.results(scope=scope, size=len(orders.index))
        self.console("Uploaded", results)
        return orders

    def filter(self, targets, /, **kwargs):
        for target in targets:
            if target.signature in self.history: continue
            with self.mutex: self.history.add(target.signature)
            yield target

    def uploader(self, targets, /, **kwargs):
        for target in targets:
            order = self.page(target=target, **kwargs)
            if order is None or bool(order.empty): continue
            order["spread"] = target.spread
            yield order

    @property
    def history(self): return self.__history
    @property
    def mutex(self): return self.__mutex


class AlpacaOrderDownloader(AlpacaDownloader, page=AlpacaOrderDownloadPage, instrument=Instrument.OPTION):
    def __call__(self, /, **kwargs):
        orders = self.page(**kwargs)
        if orders is None or bool(orders.empty): return pd.DataFrame(columns=self.columns)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        scope = self.scope(orders)
        results = self.results(scope=scope, size=len(orders.index))
        self.console("Downloaded", results)
        return orders






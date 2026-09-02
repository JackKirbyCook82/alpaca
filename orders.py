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
from types import SimpleNamespace
from datetime import date as Date
from datetime import datetime as Datetime

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
__all__ = ["AlpacaOrderUploader", "AlpacaOrderDownloader", "AlpacaOrderFile"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


status_mapping = RDict({Status.PENDING: "pending_new", Status.PARTIAL: "partially_filled", Status.CANCELING: "pending_cancel", Status.ACCEPTED: "accepted_for_bidding"})
tenure_mapping = RDict({Tenure.DAY: "day", Tenure.GTC: "gtc", Tenure.FOK: "fok"})
term_mapping = RDict({Terms.MARKET: "market", Terms.LIMIT: "limit", Terms.STOP: "stop"})
position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
intent_mapping = RDict({Intent.OPEN: "open", Intent.CLOSE: "close"})

intent_formatter = lambda value: f"{position_mapping[value.position, False]}_to_{intent_mapping[value.intent, False]}"
position_formatter = lambda position: position_mapping[position, False]
tenure_formatter = lambda tenure: tenure_mapping[tenure, False]
term_formatter = lambda term: term_mapping[term, False]
quantity_formatter = lambda quantity: f"{quantity:.0f}"
price_formatter = lambda price: f"{price:.2f}"

timestamp_parser = lambda string: pd.to_datetime(string)
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike
status_parser = lambda string: status_mapping[string, True] if string in status_mapping.values() else Status[str(string).upper()]
intent_parser = lambda string: intent_mapping[parse("{position}_to_{intent}", string)["intent"], True]
position_parser = lambda string: position_mapping[string, True]
tenure_parser = lambda string: tenure_mapping[string, True]
term_parser = lambda string: term_mapping[string, True]
quantity_parser = lambda string: abs(int(string))

order_typing = {"date": Datetime, "order": str, "asset": str, "spread": int, "ticker": str, "expire": Date, "option": int, "strike": float, "position": int, "quantity": int}
order_formatting = {"spread": int, "expire": lambda expire: expire.strftime("%Y%m%d"), "option": int, "position": int}
order_parsing = {"spread": Spread, "expire": lambda string: Datetime.strptime(string, "%Y%m%d").date(), "option": Option, "position": Position}
order_columns = ["order", "asset", "spread", "ticker", "expire", "option", "strike", "position", "quantity"]
order_header = Header(order_columns, order_typing, order_formatting, order_parsing)


class AlpacaOrderFile(File, header=order_header):
    def results(self, orders, *args, title, **kwargs):
        tickers = "|".join(list(orders["ticker"].unique()))
        expires = DateRange(list(orders["expire"].unique()))
        expires = f"{expires.minimum.strftime('%Y%m%d')}->{expires.maximum.strftime('%Y%m%d')}"
        self.console(str(title), f"Orders[{str(tickers)}, {str(expires)}, {len(orders):.0f}]")


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaOrderUploadURL(AlpacaOrderURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
class AlpacaOrderDownloadURL(AlpacaOrderURL, parameters={"limit": 500, "nested": "true", "status": "all"}, headers={"accept": "application/json"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        dates = cls.dates(*args, **kwargs)
        return tickers | dates

    @staticmethod
    def tickers(*args, tickers, **kwargs): return {"symbols": ",".join(list(tickers))}
    @staticmethod
    def dates(*args, dates, **kwargs): return {"after": dates.minimum.strftime("%Y-%m-%d"), "until": dates.maximum.strftime("%Y-%m-%d")}


class AlpacaOrderUploadPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "qty": "1"}, multiple=False, optional=False):
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
    class Date(WebJSON.Text, key="date", locator="created_at", parser=timestamp_parser): pass
    class Status(WebJSON.Text, key="status", locator="status", parser=status_parser): pass
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
    @staticmethod
    def orders(json, *args, **kwargs):
        data = AlpacaOrderData(json, *args, **kwargs)
        mapping = data(*args, **kwargs)
        records = mapping.pop("securities")
        records = [mapping | record for record in records]
        orders = pd.DataFrame.from_records(records)
        orders["expire"] = pd.to_datetime(orders["expire"])
        orders["strike"] = pd.to_numeric(orders["strike"])
        return orders[order_columns]


class AlpacaOrderUploadPage(AlpacaOrderPage):
    def __call__(self, *args, prospect, tenure, term, dryrun=False, **kwargs):
        url = AlpacaOrderUploadURL(authenticator=self.authenticator)
        securities = [{"osi": record.osi, "position": record.position, "intent": SimpleNamespace(position=record.position, intent=prospect.intent), "quantity": record.quantity} for record in prospect]
        payload = AlpacaOrderUploadPayload({"price": prospect.price, "tenure": tenure, "term": term, "securities": securities})
        if bool(dryrun):
            print("\033[34m" + pformat(str(url)) + "\033[0m")
            print("\033[34m" + pformat(url.headers) + "\033[0m")
            print("\033[34m" + pformat(payload) + "\033[0m")
            return None
        json = self.load(url, payload=payload)
        orders = self.orders(json, *args, **kwargs)
        return orders


class AlpacaOrderDownloadPage(AlpacaOrderPage):
    def __call__(self, *args, tickers, dates, **kwargs):
        parameters = dict(tickers=tickers, dates=dates)
        url = AlpacaOrderDownloadURL(authenticator=self.authenticator, **parameters)
        json = self.load(url, payload=None)
        orders = self.orders(json, *args, **kwargs)
        return orders


class AlpacaOrderUploader(WebStream, Logging, page=AlpacaOrderUploadPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()

    def __call__(self, prospects, /, **kwargs):
        assert isinstance(prospects, list)
        if not bool(prospects): return pd.DataFrame(columns=order_columns)
        prospects = self.filter(prospects, **kwargs)
        prospects = list(prospects)
        if not bool(prospects): return pd.DataFrame(columns=order_columns)
        orders = self.uploader(prospects, **kwargs)
        orders = list(orders)
        if not bool(orders): return pd.DataFrame(columns=order_columns)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        scope = self.scope(orders, instruments=Instrument.OPTION)
        self.results(scope=scope, size=len(orders.index), title="Uploaded")
        return orders

    def filter(self, prospects, /, **kwargs):
        for prospect in prospects:
            if prospect.signature in self.history: continue
            with self.mutex: self.history.add(prospect.signature)
            yield prospect

    def uploader(self, prospects, /, **kwargs):
        for prospect in prospects:
            order = self.page(prospect=prospect, **kwargs)
            order["spread"] = prospect.spread
            if order is None: continue
            if bool(order.empty): continue
            securities = [f"{str(record.osi)}={int(record.position) * int(record.quantity):.0f}" for record in prospect]
            self.console("Uploaded", f"Prospect[{', '.join(securities)}]")
            self.console("Uploaded", f"Prospect[Moneyness={prospect.moneyness:+.2f}, Tightness={prospect.tightness:+.2f}, Activity={prospect.activity:+.2f}]")
            self.console("Uploaded", f"Prospect[ZSpread={prospect.zspread:+.2f}, Multiple={prospect.multiple:+.2f}, Ratio={prospect.ratio:+.2f}]")
            yield order

    @property
    def history(self): return self.__history
    @property
    def mutex(self): return self.__mutex


class AlpacaOrderDownloader(WebStream, Logging, page=AlpacaOrderDownloadPage):
    def __call__(self, /, **kwargs):
        orders = self.page(**kwargs)
        orders = list(orders)
        if not bool(orders): return pd.DataFrame(columns=order_columns)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["order", "asset"], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        scope = self.scope(orders, instruments=Instrument.OPTION)
        self.results(scope=scope, size=len(orders.index), title="Downloaded")
        return orders




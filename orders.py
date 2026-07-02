# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

from pprint import pformat
from abc import ABC, abstractmethod

from finance.variables import Enumerations
from finance.logging import Logging
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webpayloads import WebPayload
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaSpreadUploader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


tenure_parser = lambda tenure: {Enumerations.Tenure.DAY: "day", Enumerations.Tenure.GTC: "gtc", Enumerations.Tenure.FOK: "fok"}[tenure]
term_parser = lambda term: {Enumerations.Terms.MARKET: "market", Enumerations.Terms.LIMIT: "limit", Enumerations.Terms.STOP: "stop"}[term]
position_parser = lambda position: {Enumerations.Position.LONG: "buy", Enumerations.Position.SHORT: "sell"}[position]
intent_parser = lambda position: {Enumerations.Position.LONG: "buy_to_open", Enumerations.Position.SHORT: "sell_to_open"}[position]
quantity_parser = lambda value: str(abs(value))
cost_parser = lambda value: f"{value:.2f}"


class AlpacaOrderURL(WebURL, headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaSpreadURL(AlpacaOrderURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    pass


class AlpacaSpreadPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "qty": "1"}):
    class Cost(WebPayload.Value, key="cost", locator="limit_price", parser=cost_parser): pass
    class Tenure(WebPayload.Value, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Terms(WebPayload.Value, key="term", locator="type", parser=term_parser): pass
    class Legs(WebPayload.Mapping, key="securities", locator="legs", multiple=True):
        class Osi(WebPayload.Value, key="osi", locator="symbol"): pass
        class Position(WebPayload.Value, key="position", locator="side", parser=position_parser): pass
        class Intent(WebPayload.Value, key="intent", locator="position_intent", parser=intent_parser): pass
        class Quantity(WebPayload.Value, key="quantity", locator="ratio_qty", parser=quantity_parser): pass


class AlpacaOrderPage(WebJSONPage, ABC): pass
class AlpacaSpreadPage(AlpacaOrderPage):
    def __init__(self, *args, uploading=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.__uploading = bool(uploading)

    def __call__(self, *args, spread, tenure, term, **kwargs):
        keys = ["osi", "position", "intent", "quantity"]
        records = zip(spread.osi, spread.position, spread.position, spread.quantity)
        securities = [dict(zip(keys, values)) for values in records]
        sources = dict(cost=spread.cost, tenure=tenure, term=term, securities=securities)
        url = AlpacaSpreadURL(authenticator=self.authenticator)
        payload = AlpacaSpreadPayload(sources)
        if self.uploading: self.load(url, payload=payload)
        else: print("\033[31m" + pformat(url) + "\n" + pformat(payload) + "\033[0m")

    @property
    def uploading(self): return self.__uploading


class AlpacaOrderUploader(WebStream, Logging, ABC):
    @abstractmethod
    def uploader(self, *args, **kwargs): pass


class AlpacaSpreadUploader(AlpacaOrderUploader, page=AlpacaSpreadPage):
    def __call__(self, spreads, *args, **kwargs):
        assert isinstance(spreads, list)
        if not bool(spreads): return
        self.uploader(spreads, *args, **kwargs)

    def uploader(self, spreads, *args, **kwargs):
        for spread in spreads:
            self.page(*args, spread=spread, **kwargs)
            self.console("Updated", f"Spread[{', '.join(spread.osi)}]")
            self.console("Updated", f"Spread[Tight={spread.tightness:.2f}, Money={spread.moneyness:.2f}, Active={spread.activity:.2f}]")
        self.results(spreads, title="Uploaded", instrument=Enumerations.Instrument.SPREAD)





# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

import multiprocessing
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
intent_parser = lambda position, intent: str(position_parser[position]) + "to" + str(intent)
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
        class Intent(WebPayload.Value, key="intent", locator="position_intent", parser=intent_parser): pass
        class Position(WebPayload.Value, key="position", locator="side", parser=position_parser): pass
        class Quantity(WebPayload.Value, key="quantity", locator="ratio_qty", parser=quantity_parser): pass


class AlpacaOrderPage(WebJSONPage, ABC): pass
class AlpacaSpreadPage(AlpacaOrderPage):
    def __call__(self, *args, spread, tenure, term, intent, **kwargs):
        securities = [{"osi": record.osi, "position": record.position, "intent": (record.postion, intent), "quantity": record.quantity} for record in spread.records]
        sources = dict(cost=spread.cost, tenure=tenure, term=term, securities=securities)
        url = AlpacaSpreadURL(authenticator=self.authenticator)
        payload = AlpacaSpreadPayload(sources)
        if not bool(self.safemode): self.load(url, payload=payload)
        else: print("\033[31m" + pformat(str(url)) + "\n" + pformat(payload) + "\033[0m")


class AlpacaOrderUploader(WebStream, Logging, ABC):
    def __init__(self, *args, file, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()
        self.__file = file

    @abstractmethod
    def uploader(self, *args, **kwargs): pass

    @property
    def history(self): return self.__history
    @property
    def mutex(self): return self.__mutex
    @property
    def file(self): return self.__file


class AlpacaSpreadUploader(AlpacaOrderUploader, page=AlpacaSpreadPage):
    def __call__(self, spreads, /, **kwargs):
        assert isinstance(spreads, list)
        if not bool(spreads): return
        generator = self.generator(spreads, **kwargs)
        spreads = list(generator)
        if not bool(spreads): return
        self.uploader(spreads, **kwargs)

    def generator(self, spreads, /, **kwargs):
        for spread in spreads:
            if spread.signature in self.history: continue
            with self.mutex: self.history.add(spread.signature)
            yield spread

    def uploader(self, spreads, /, **kwargs):
        for spread in spreads:
            self.page(spread=spread, **kwargs)
            securities = [f"{str(record.osi)}={int(record.position) * int(record.quantity):.0f}" for record in spread.records]
            self.console("Updated", f"Spread[{', '.join(securities)}]")
            self.console("Updated", f"Spread[Tight={spread.tightness:.2f}, Money={spread.moneyness:.2f}, Active={spread.activity:.2f}]")
        self.results(spreads, title="Uploaded", instrument=Enumerations.Instrument.SPREAD)




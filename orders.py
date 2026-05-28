# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
from abc import ABC, abstractmethod

from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webpayloads import WebPayload
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaSpreadUploader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


cost_parser = lambda value: str(np.negative(value))
tenure_parser = lambda tenure: {Concepts.Markets.Tenure.DAY: "day"}[tenure]
term_parser = lambda term: {Concepts.Markets.Term.LIMIT: "limit"}[term]
position_parser = lambda tenure: {Concepts.Securities.Position.LONG: "buy", Concepts.Securities.Position.SHORT: "sell"}[tenure]
quantity_parser = lambda value: str(abs(value))


class AlpacaOrderURL(WebURL, headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaSpreadURL(AlpacaOrderURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    pass


class AlpacaSpreadPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "extended_hours": False, "qty": "1"}):
    class Cost(WebPayload.Text, key="cost", locator="limit_price", parser=cost_parser): pass
    class Tenure(WebPayload.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Terms(WebPayload.Text, key="term", locator="type", parser=term_parser): pass
    class Legs(WebPayload.Mapping, key="securities", locator="legs", multiple=True):
        class Osi(WebPayload.Text, key="osi", locator="symbol"): pass
        class Position(WebPayload.Text, key="position", locator="side", parser=position_parser): pass
        class Quantity(WebPayload.Text, key="quantity", locator="ratio_qty", parser=quantity_parser): pass


class AlpacaOrderPage(WebJSONPage, ABC): pass
class AlpacaSpreadPage(AlpacaOrderPage):
    def __call__(self, *args, spread, tenure, term, **kwargs):
        keys, records = ["osi", "position", "quantity"], zip(spread.osi, spread.position, spread.quantity)
        securities = [dict(zip(keys, values)) for values in records]
        sources = dict(cost=spread.cost, tenure=tenure, term=term, securities=securities)
        url = AlpacaSpreadURL(authenticator=self.authenticator)
        payload = AlpacaSpreadPayload(sources)
        self.load(url, payload=payload)


class AlpacaOrderUploader(WebStream, Alerting, ABC):
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
            self.alert(spread, title="Uploaded", instrument=Concepts.Securities.Instrument.SPREAD)




# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

import multiprocessing
import pandas as pd
from parse import parse
from abc import ABC, abstractmethod

from finance.variables import Enumerations
from finance.logging import Logging
from finance.osi import OSI
from support.custom import ReversibleDict as RDict
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webpayloads import WebPayload
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader", "AlpacaOrderDownloader", "AlpacaOrder"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


tenure_mapping = RDict({Enumerations.Tenure.DAY: "day", Enumerations.Tenure.GTC: "gtc", Enumerations.Tenure.FOK: "fok"})
term_mapping = RDict({Enumerations.Terms.MARKET: "market", Enumerations.Terms.LIMIT: "limit", Enumerations.Terms.STOP: "stop"})
position_mapping = RDict({Enumerations.Position.LONG: "buy", Enumerations.Position.SHORT: "sell"})
intent_mapping = RDict({Enumerations.Intent.OPEN: "open", Enumerations.Intent.CLOSE: "close"})

intent_formatter = lambda position, intent: f"{position[position, False]}_to_{intent_mapping[intent, False]}"
position_formatter = lambda position: position_mapping[position, False]
tenure_formatter = lambda tenure: tenure_mapping[tenure, False]
term_formatter = lambda term: intent_mapping[term, False]
quantity_formatter = lambda quantity: f"{quantity:.0f}"
cost_formatter = lambda cost: f"{cost:.2f}"

timestamp_parser = lambda string: pd.to_datetime(string)
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike
intent_parser = lambda string: intent_mapping[parse("{position}_to_{intent}", string)["intent"], True]
position_parser = lambda string: position_mapping[string, True]
tenure_parser = lambda string: tenure_mapping[string, True]
term_parser = lambda string: term_mapping[string, True]
quantity_parser = lambda string: int(string)


AlpacaOrder = ["identity", "created", "submitted", "filled", "expired", "canceled", "failed", "status", "tenure", "term", "asset", "ticker", "expire", "option", "strike", "position", "quantity"]
class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaUploadingOrder(WebURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
# class AlpacaDownloadingOrder(WebURL, parameters={"status": "all", "nested": True}, headers={"accept": "application/json"}):
#     @staticmethod
#     def path(*args, strategy, **kwargs): return [str(strategy)]


class AlpacaOrderPayload(WebPayload.Mapping, mapping={"order_class": "mleg", "qty": "1"}, multiple=False, optional=False):
    class Cost(WebPayload.Value, key="cost", locator="limit_price", parser=cost_formatter): pass
    class Tenure(WebPayload.Value, key="tenure", locator="time_in_force", parser=tenure_formatter): pass
    class Terms(WebPayload.Value, key="term", locator="type", parser=term_formatter): pass
    class Securities(WebPayload.Mapping, key="securities", locator="legs", multiple=True, optional=False):
        class Osi(WebPayload.Value, key="osi", locator="symbol"): pass
        class Intent(WebPayload.Value, key="intent", locator="position_intent", parser=intent_formatter): pass
        class Position(WebPayload.Value, key="position", locator="side", parser=position_formatter): pass
        class Quantity(WebPayload.Value, key="quantity", locator="ratio_qty", parser=quantity_formatter): pass


class AlpacaOrderData(WebJSON.Mapping, multiple=False, optional=False):
    class Collective(WebJSON.Text, key="collective", locator="id", parser=str): pass
    class Created(WebJSON.Text, key="created", locator="created_at", parser=timestamp_parser): pass
    class Submitted(WebJSON.Text, key="sumbitted", locator="submitted_at", parser=timestamp_parser): pass
    class Filled(WebJSON.Text, key="filled", locator="filled_at", parser=timestamp_parser): pass
    class Expired(WebJSON.Text, key="expired", locator="expired_at", parser=timestamp_parser): pass
    class Canceled(WebJSON.Text, key="canceled", locator="canceled_at", parser=timestamp_parser): pass
    class Failed(WebJSON.Text, key="failed", locator="failed_at", parser=timestamp_parser): pass
    class Status(WebJSON.Text, key="status", locator="status", parser=Enumerations.Status): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass
    class Securities(WebJSON.Mapping, key="securities", locator="legs", parser=dict, multiple=True, optional=False):
        class Individual(WebJSON.Text, key="individual", locator="asset_id", parser=str): pass
        class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
        class Expire(WebJSON.Text, key="expire", locator="expire", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="option", parser=option_parser): pass
        class Strike(WebJSON.Text, key="strike", locator="strike", parser=strike_parser): pass
        class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
        class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=int): pass


class AlpacaOrderPage(WebJSONPage, ABC):
    def __call__(self, *args, **kwargs):
        data = self.execute(*args, **kwargs)
        mapping = data(*args, **kwargs)
        records = mapping.pop("securities")
        records = [mapping | record for record in records]
        order = pd.DataFrame.from_records(records)
        return order

    @abstractmethod
    def execute(self, *args, **kwargs): pass


class AlpacaUploadingOrderPage(AlpacaOrderPage):
    def execute(self, *args, prospect, tenure, term, intent, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        url = AlpacaUploadingOrder(**parameters)
        securities = [{"osi": record.osi, "position": record.position, "intent": (record.postion, intent), "quantity": record.quantity} for record in spread.records]
        payload = AlpacaOrderData({"cost": prospect.cost, "tenure": tenure, "term": term, "securities": securities})
        json = self.load(url, payload=payload)
        data = AlpacaOrderData(json, *args, **kwargs)
        return data


class AlpacaDownloadingOrderPage(AlpacaOrderPage):
    pass

#    def execute(self, *args, strategy, **kwargs):
#        parameters = dict(authenticator=self.authenticator)
#        url = AlpacaDownloadingOrder(strategy=strategy, **parameters)
#        json = self.load(url)
#        data = AlpacaOrderData(json, *args, **kwargs)
#        return data


class AlpacaOrderUploader(WebStream, Logging, page=AlpacaUploadingOrderPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__mutex = multiprocessing.Lock()
        self.__history = set()

    def __call__(self, prospects, /, **kwargs):
        assert isinstance(prospects, list)
        if not bool(prospects): return pd.DataFrame(columns=AlpacaOrder)
        generator = self.generator(prospects, **kwargs)
        prospects = list(generator)
        if not bool(prospects): return pd.DataFrame(columns=AlpacaOrder)
        orders = self.uploader(prospects, **kwargs)
        orders = pd.concat(list(orders), axis=0)
        orders = orders.sort_values(by=["identity", "assert"], ascending=[True, True], inplace=False)
        orders = orders.reset_index(drop=True, inplace=False)
        self.results(orders, title="Uploaded", instrument=Enumerations.Instrument.OPTION)
        return orders

    def generator(self, prospects, /, **kwargs):
        for prospect in prospects:
            if prospect.signature in self.history: continue
            with self.mutex: self.history.add(prospect.signature)
            yield prospect

    def uploader(self, prospects, /, **kwargs):
        for prospect in prospects:
            order = self.page(prospect=prospect, **kwargs)
            securities = [f"{str(record.osi)}={int(record.position) * int(record.quantity):.0f}" for record in prospect.records]
            self.console("Updated", f"Propsect[{', '.join(securities)}]")
            self.console("Updated", f"Prospect[Tight={prospect.tightness:.2f}, Money={prospect.moneyness:.2f}, Active={prospect.activity:.2f}]")
            yield order

    @property
    def history(self): return self.__history
    @property
    def mutex(self): return self.__mutex


class AlpacaOrderDownloader(WebStream, Logging, page=AlpacaDownloadingOrderPage):
    pass

#    def __call__(self, strategies, **kwargs):
#        assert isinstance(strategies, list)
#        if not bool(strategies): return pd.DataFrame(columns=AlpacaOrder)
#        orders = self.downloader(strategies, **kwargs)
#        orders = pd.concat(list(orders), axis=0)
#        orders = orders.sort_values(by=["identity", "asset"], ascending=[True, True], inplace=False)
#        orders = orders.reset_index(drop=True, inplace=False)
#        self.results(orders, title="Downloaded", instrument=Enumerations.Instrument.OPTION)
#        return orders

#    def downloader(self, strategies, /, **kwargs):
#        for strategy in strategies:
#            order = self.page(strategy=strategy, **kwargs)
#            yield order


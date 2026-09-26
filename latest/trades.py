# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Latest Trades Objects
@author: Jack Kirby Cook
@file:   alpaca/latest/trades.py

"""

import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict

from finance.enumerations import Instrument
from finance.reporting import Results
from finance.osi import OSI
from webscraping.webpages import WebJSONPage, WebStream
from webscraping.weburl import WebURL
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockTradesLatestDownloader", "AlpacaOptionTradesLatestDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaTradesLatestURL(WebURL, domain="https://data.alpaca.markets", path=["v2"], headers={"accept": "application/json"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        products = cls.products(*args, **kwargs)
        return products

    @staticmethod
    def products(*args, products, **kwargs): raise NotImplementedError()

    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaStockTradesLatestURL(AlpacaTradesLatestURL, path=["stocks", "trades", "latest"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionTradesLatestURL(AlpacaTradesLatestURL, path=["v1beta1", "options", "trades", "latest"], parameters={"feed": "indicative"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaTradesLatestPage(WebJSONPage, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("datetime", "t", lambda string: pd.to_datetime(string, utc=True)), AlpacaField("trade", "p", np.float32), AlpacaField("size", "s", np.float32)]
        parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}
        self.__fields = fields
        self.__parser = parser

    def __call__(self, *args, products, history, **kwargs):
        parameters = dict(products=products, history=history, authenticator=self.authenticator)
        records = self.execute(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = [{"product": product} | self.parser(mapping) for product, contents in json["trades"].items() for mapping in contents]
        return records

    @staticmethod
    @abstractmethod
    def url(*args, **kwargs): pass

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser


class AlpacaStockTradesLatestPage(AlpacaTradesLatestPage):
    @staticmethod
    def url(*args, **kwargs): return AlpacaStockTradesLatestURL(*args, **kwargs)

class AlpacaOptionTradesLatestPage(AlpacaTradesLatestPage):
    @staticmethod
    def url(*args, **kwargs): return AlpacaOptionTradesLatestURL(*args, **kwargs)


class AlpacaTradesLatestDownloader(WebStream, Results, Logging, ABC):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        bars = self.downloader(products, **kwargs)
        bars = pd.concat(list(bars), axis=0)
        bars = self.parser(bars, **kwargs)
        return bars

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            bars = self.page(products=products, **kwargs)
            if bars is None or bool(bars.empty): continue
            results = self.results(scope=scope, size=len(bars))
            self.console("Downloaded", results)
            yield bars

    @staticmethod
    @abstractmethod
    def parser(bars, /, **kwargs): pass


class AlpacaStockTradesLatestDownloader(AlpacaTradesLatestDownloader, page=AlpacaStockTradesLatestPage):
    def scope(self, products, **kwargs):
        return super().scope(products, instrument=Instrument.STOCK)

    @staticmethod
    def parser(bars, /, **kwargs):
        bars["datetime"] = pd.to_datetime(bars["datetime"])
        bars = bars.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "ticker"})
        bars = bars.reset_index(drop=True, inplace=False)
        return bars


class AlpacaOptionTradesLatestDownloader(AlpacaTradesLatestDownloader, page=AlpacaOptionTradesLatestPage):
    def scope(self, products, **kwargs):
        return super().scope(products, instrument=Instrument.OPTION)

    @staticmethod
    def parser(bars, /, **kwargs):
        bars["datetime"] = pd.to_datetime(bars["datetime"])
        bars = bars.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(bars["osi"].map(OSI).map(asdict), index=bars.index)
        bars = pd.concat([bars, contracts], axis=1)
        bars = bars.reset_index(drop=True, inplace=False)
        return bars



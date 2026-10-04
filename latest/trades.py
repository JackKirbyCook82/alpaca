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
from webscraping.webpages import WebJSONPage
from webscraping.weburl import WebURL
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockTradesLatestDownloader", "AlpacaOptionTradesLatestDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


options_columns = ["ticker", "expire", "option", "strike", "datatime", "trade", "size"]
stocks_columns = ["ticker", "datetime", "trade", "size"]


class AlpacaTradesLatestURL(WebURL, ABC, domain="https://data.alpaca.markets", headers={"accept": "application/json"}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        return products

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaStockTradesLatestURL(AlpacaTradesLatestURL, path=["v2", "stocks", "trades", "latest"], parameters={"feed": "delayed_sip"}):
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
        records = [{"product": product} | self.parser(mapping) for product, mapping in json["trades"].items()]
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


class AlpacaTradesLatestDownloader(Results, Logging, ABC):
    def __init_subclass__(cls, /, page, columns, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.Columns = columns
        cls.Page = page

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = type(self).Page(*args, **kwargs)
        self.__columns = list(type(self).Columns)

    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        trades = self.downloader(products, **kwargs)
        if not trades: return pd.DataFrame(columns=self.columns)
        trades = pd.concat(list(trades), axis=0)
        trades = self.parser(trades, **kwargs)
        return trades

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            trades = self.page(products=products, **kwargs)
            if trades is None or bool(trades.empty): continue
            results = self.results(scope=scope, size=len(trades))
            self.console("Downloaded", results)
            yield trades

    @staticmethod
    @abstractmethod
    def parser(trades, /, **kwargs): pass

    @property
    def columns(self): return self.__columns
    @property
    def page(self): return self.__page


class AlpacaStockTradesLatestDownloader(AlpacaTradesLatestDownloader, page=AlpacaStockTradesLatestPage, stocks=stocks_columns):
    def scope(self, products, **kwargs): return super().scope(products, instrument=Instrument.STOCK)

    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "ticker"})
        trades = trades.reset_index(drop=True, inplace=False)
        return trades


class AlpacaOptionTradesLatestDownloader(AlpacaTradesLatestDownloader, page=AlpacaOptionTradesLatestPage, columns=options_columns):
    def scope(self, products, **kwargs): return super().scope(products, instrument=Instrument.OPTION)

    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(trades["osi"].map(OSI).map(asdict), index=trades.index)
        trades = pd.concat([trades, contracts], axis=1)
        trades = trades.reset_index(drop=True, inplace=False)
        return trades




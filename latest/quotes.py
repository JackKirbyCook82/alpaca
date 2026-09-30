# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Latest Quotes Objects
@author: Jack Kirby Cook
@file:   alpaca/latest/quotes.py

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
__all__ = ["AlpacaStockQuotesLatestDownloader", "AlpacaOptionQuotesLatestDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


options_columns = ["ticker", "expire", "option", "strike", "datatime", "bid", "ask", "supply", "demand"]
stocks_columns = ["ticker", "datetime", "bid", "ask", "supply", "demand"]


class AlpacaQuotesLatestURL(WebURL, domain="https://data.alpaca.markets", headers={"accept": "application/json"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        products = cls.products(*args, **kwargs)
        return products

    @staticmethod
    def products(*args, products, **kwargs): raise NotImplementedError()

    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaStockQuotesLatestURL(AlpacaQuotesLatestURL, path=["v2", "stocks", "quotes", "latest"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionQuotesLatestURL(AlpacaQuotesLatestURL, path=["v1beta1", "options", "quotes", "latest"], parameters={"feed": "indicative"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaQuotesLatestPage(WebJSONPage, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("bid", "bp", np.float32), AlpacaField("ask", "ap", np.float32)]
        fields = fields + [AlpacaField("supply", "as", np.float32), AlpacaField("demand", "bs", np.float32)]
        fields = fields + [AlpacaField("datetime", "t", lambda string: pd.to_datetime(string, utc=True))]
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
        records = [{"product": product} | self.parser(mapping) for product, mapping in json["quotes"].items()]
        return records

    @staticmethod
    @abstractmethod
    def url(*args, **kwargs): pass

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser


class AlpacaStockQuotesLatestPage(AlpacaQuotesLatestPage):
    @staticmethod
    def url(*args, **kwargs): return AlpacaStockQuotesLatestURL(*args, **kwargs)

class AlpacaOptionQuotesLatestPage(AlpacaQuotesLatestPage):
    @staticmethod
    def url(*args, **kwargs): return AlpacaOptionQuotesLatestURL(*args, **kwargs)


class AlpacaQuotesLatestDownloader(Results, Logging, ABC):
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
        quotes = self.downloader(products, **kwargs)
        if not quotes: return pd.DataFrame(columns=self.columns)
        quotes = pd.concat(list(quotes), axis=0)
        quotes = self.parser(quotes, **kwargs)
        return quotes

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            quotes = self.page(products=products, **kwargs)
            if quotes is None or bool(quotes.empty): continue
            results = self.results(scope=scope, size=len(quotes))
            self.console("Downloaded", results)
            yield quotes

    @staticmethod
    @abstractmethod
    def parser(quotes, /, **kwargs): pass

    @property
    def columns(self): return self.__columns
    @property
    def page(self): return self.__page


class AlpacaStockQuotesLatestDownloader(AlpacaQuotesLatestDownloader, page=AlpacaOptionQuotesLatestPage, columns=stocks_columns):
    def scope(self, products, **kwargs):
        return super().scope(products, instrument=Instrument.STOCK)

    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "ticker"})
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes


class AlpacaOptionQuotesLatestDownloader(AlpacaQuotesLatestDownloader, page=AlpacaOptionQuotesLatestPage, columns=stocks_columns):
    def scope(self, products, **kwargs):
        return super().scope(products, instrument=Instrument.OPTION)

    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(quotes["osi"].map(OSI).map(asdict), index=quotes.index)
        quotes = pd.concat([quotes, contracts], axis=1)
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes




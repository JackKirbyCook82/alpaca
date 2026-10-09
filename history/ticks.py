# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca History Tick Objects
@author: Jack Kirby Cook
@file:   alpaca/history/trades.py

"""

import numpy as np
import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader, AlpacaField
from finance.enumerations import Instrument
from finance.osi import OSI
from webscraping.webdatas import WebJSON

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockTicksHistoryDownloader", "AlpacaOptionTicksHistoryDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


pagination_parser = lambda string: str(string) if string != "None" else None
history_parser = lambda string: pd.to_datetime(string, utc=True)
options_columns = ["ticker", "expire", "option", "strike", "datetime", "trade", "size"]
stocks_columns = ["ticker", "datetime", "trade", "size"]


class AlpacaTicksHistoryURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets", parameters={"limit": 10000}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        history = self.history(*args, **kwargs)
        pagination = self.pagination(*args, **kwargs)
        return products | history | pagination

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass


class AlpacaStockTicksHistoryURL(AlpacaTicksHistoryURL, path=["v2", "stocks", "trades"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionTicksHistoryURL(AlpacaTicksHistoryURL, path=["v1beta1", "options", "trades"]):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaTicksHistoryData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, optional=True): pass


class AlpacaTicksHistoryPage(AlpacaDownloadPage, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("datetime", "t", history_parser), AlpacaField("trade", "p", np.float32), AlpacaField("size", "s", np.int64)]
        parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}
        self.__fields = fields
        self.__parser = parser

    def __call__(self, *args, products, history, **kwargs):
        parameters = dict(products=products, history=history)
        records = self.execute(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, pagination=None, **kwargs):
        url = self.url(*args, pagination=pagination, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = [{"product": product} | self.parser(mapping) for product, contents in json["trades"].items() for mapping in contents]
        data = self.data(json, *args, **kwargs)
        pagination = data["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.execute(*args, pagination=pagination, **kwargs)

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser


class AlpacaStockTicksHistoryPage(AlpacaTicksHistoryPage, url=AlpacaStockTicksHistoryURL, data=AlpacaTicksHistoryData): pass
class AlpacaOptionTicksHistoryPage(AlpacaTicksHistoryPage, url=AlpacaOptionTicksHistoryURL, data=AlpacaTicksHistoryData): pass


class AlpacaTicksHistoryDownloader(AlpacaDownloader):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        trades = list(self.downloader(products, **kwargs))
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


class AlpacaStockTicksHistoryDownloader(AlpacaTicksHistoryDownloader, page=AlpacaStockTicksHistoryPage, columns=stocks_columns, instrument=Instrument.STOCK):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "ticker"})
        trades = trades.reset_index(drop=True, inplace=False)
        return trades


class AlpacaOptionTicksHistoryDownloader(AlpacaTicksHistoryDownloader, page=AlpacaOptionTicksHistoryPage, columns=options_columns, instrument=Instrument.OPTION):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(trades["osi"].map(OSI).map(asdict), index=trades.index)
        trades = pd.concat([trades, contracts], axis=1)
        trades = trades.reset_index(drop=True, inplace=False)
        return trades




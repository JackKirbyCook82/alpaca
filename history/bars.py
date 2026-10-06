# -*- coding: utf-8 -*-
"""
Created on Thurs Mar 26 2026
@name:   Alpaca History Bars Objects
@author: Jack Kirby Cook
@file:   alpaca/history/bars.py

"""

import numpy as np
import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader, AlpacaField
from finance.enumerations import Instrument, Frequency
from finance.osi import OSI
from webscraping.webdatas import WebJSON

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockBarsHistoryDownloader", "AlpacaOptionBarsHistoryDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


frequency_mapping = {Frequency.MINUTELY: "T", Frequency.HOURLY: "H", Frequency.DAILY: "D", Frequency.WEEKLY: "W", Frequency.MONTHLY: "M", Frequency.YEARLY: "Y"}
frequency_parser = lambda frequency: f"{int(frequency.duration)}{frequency_mapping[frequency.by]}"
pagination_parser = lambda string: str(string) if string != "None" else None
history_parser = lambda string: pd.to_datetime(string, utc=True)
options_columns = ["ticker", "expire", "option", "strike", "datetime", "open", "close", "high", "low", "volume"]
stocks_columns = ["ticker", "datetime", "open", "close", "high", "low", "volume"]


class AlpacaBarsHistoryURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets", parameters={"limit": 10000}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        history = self.history(*args, **kwargs)
        pagination = self.pagination(*args, **kwargs)
        frequency = (self.frequency(*args, **kwargs))
        return products | frequency | history | pagination

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass
    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if pagination is not None else {}

    @staticmethod
    def frequency(*args, frequency, **kwargs):
        assert isinstance(frequency.duration, int)
        if frequency.by == Frequency.MINUTELY: assert 1 <= frequency.duration <= 59
        if frequency.by == Frequency.HOURLY: assert 1 <= frequency.duration <= 23
        if frequency.by == Frequency.DAILY: assert frequency.duration == 1
        if frequency.by == Frequency.WEEKLY: assert frequency.duration == 1
        if frequency.by == Frequency.MONTHLY: assert 1 <= frequency.duration <= 12 and 12 % frequency.duration == 0
        assert frequency.by != Frequency.YEARLY
        return {"timeframe": frequency_parser(frequency)}


class AlpacaStockBarsHistoryURL(AlpacaBarsHistoryURL, path=["v2", "stocks", "bars"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionBarsHistoryURL(AlpacaBarsHistoryURL, path=["v1beta1", "options", "bars"]):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaBarsHistoryData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, optional=True): pass


class AlpacaBarsHistoryPage(AlpacaDownloadPage, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("open", "o", np.float32), AlpacaField("close", "c", np.float32), AlpacaField("high", "h", np.float32), AlpacaField("low", "l", np.float32)]
        fields = fields + [AlpacaField("datetime", "t", history_parser), AlpacaField("volume", "v", np.int64)]
        parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}
        self.__fields = fields
        self.__parser = parser

    def __call__(self, *args, products, frequency, history, **kwargs):
        parameters = dict(products=products, frequency=frequency, history=history)
        records = self.execute(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, pagination=None, **kwargs):
        url = self.url(*args, pagination=pagination, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = [{"product": product} | self.parser(mapping) for product, contents in json["bars"].items() for mapping in contents]
        data = self.data(json, *args, **kwargs)
        pagination = data["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.execute(*args, pagination=pagination, **kwargs)

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser


class AlpacaStockBarsHistoryPage(AlpacaBarsHistoryPage, url=AlpacaStockBarsHistoryURL, data=AlpacaBarsHistoryData): pass
class AlpacaOptionBarsHistoryPage(AlpacaBarsHistoryPage, url=AlpacaOptionBarsHistoryURL, data=AlpacaBarsHistoryData): pass


class AlpacaBarsHistoryDownloader(AlpacaDownloader):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        bars = list(self.downloader(products, **kwargs))
        if not bars: return pd.DataFrame(columns=self.columns)
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


class AlpacaStockBarsHistoryDownloader(AlpacaBarsHistoryDownloader, page=AlpacaStockBarsHistoryPage, columns=stocks_columns, instrument=Instrument.STOCK):
    @staticmethod
    def parser(bars, /, **kwargs):
        bars["datetime"] = pd.to_datetime(bars["datetime"])
        bars = bars.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "ticker"})
        bars = bars.reset_index(drop=True, inplace=False)
        return bars


class AlpacaOptionBarsHistoryDownloader(AlpacaBarsHistoryDownloader, page=AlpacaOptionBarsHistoryPage, columns=options_columns, instrument=Instrument.OPTION):
    @staticmethod
    def parser(bars, /, **kwargs):
        bars["datetime"] = pd.to_datetime(bars["datetime"])
        bars = bars.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(bars["osi"].map(OSI).map(asdict), index=bars.index)
        bars = pd.concat([bars, contracts], axis=1)
        bars = bars.reset_index(drop=True, inplace=False)
        return bars





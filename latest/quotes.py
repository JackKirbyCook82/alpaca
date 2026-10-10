# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Latest Quotes Objects
@author: Jack Kirby Cook
@file:   alpaca/latest/quotes.py

"""

import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader
from finance.enumerations import Instrument
from finance.osi import OSI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockQuotesLatestDownloader", "AlpacaOptionQuotesLatestDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


options_columns = ["ticker", "expire", "option", "strike", "datetime", "bid", "ask", "supply", "demand"]
stocks_columns = ["ticker", "datetime", "bid", "ask", "supply", "demand"]


class AlpacaQuotesLatestURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets", headers={"accept": "application/json"}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        return products

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass


class AlpacaStockQuotesLatestURL(AlpacaQuotesLatestURL, path=["v2", "stocks", "quotes", "latest"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionQuotesLatestURL(AlpacaQuotesLatestURL, path=["v1beta1", "options", "quotes", "latest"], parameters={"feed": "indicative"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaQuotesLatestPage(AlpacaDownloadPage, ABC, fields=["datetime", "bid", "ask", "supply", "demand"]):
    def __call__(self, *args, products, **kwargs):
        parameters = dict(products=products)
        records = self.execute(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = self.records(json["quotes"], *args, **kwargs)
        return records


class AlpacaStockQuotesLatestPage(AlpacaQuotesLatestPage, url=AlpacaStockQuotesLatestURL): pass
class AlpacaOptionQuotesLatestPage(AlpacaQuotesLatestPage, url=AlpacaOptionQuotesLatestURL): pass


class AlpacaQuotesLatestDownloader(AlpacaDownloader):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        quotes = list(self.downloader(products, **kwargs))
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


class AlpacaStockQuotesLatestDownloader(AlpacaQuotesLatestDownloader, page=AlpacaStockQuotesLatestPage, columns=stocks_columns, instrument=Instrument.STOCK):
    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "ticker"})
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes


class AlpacaOptionQuotesLatestDownloader(AlpacaQuotesLatestDownloader, page=AlpacaOptionQuotesLatestPage, columns=options_columns, instrument=Instrument.OPTION):
    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(quotes["osi"].map(OSI).map(asdict), index=quotes.index)
        quotes = pd.concat([quotes, contracts], axis=1)
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes




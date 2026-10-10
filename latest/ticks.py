# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Latest Ticks Objects
@author: Jack Kirby Cook
@file:   alpaca/latest/trades.py

"""

import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader
from finance.enumerations import Instrument
from finance.osi import OSI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockTicksLatestDownloader", "AlpacaOptionTicksLatestDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


options_columns = ["ticker", "expire", "option", "strike", "datetime", "trade", "size"]
stocks_columns = ["ticker", "datetime", "trade", "size"]


class AlpacaTicksLatestURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets", headers={"accept": "application/json"}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        return products

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass


class AlpacaStockTicksLatestURL(AlpacaTicksLatestURL, path=["v2", "stocks", "trades", "latest"], parameters={"feed": "delayed_sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionTicksLatestURL(AlpacaTicksLatestURL, path=["v1beta1", "options", "trades", "latest"], parameters={"feed": "indicative"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaTicksLatestPage(AlpacaDownloadPage, ABC, fields=["datetime", "trade", "size"]):
    def __call__(self, *args, products, **kwargs):
        parameters = dict(products=products)
        records = self.execute(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = self.records(json["trades"], *args, **kwargs)
        return records


class AlpacaStockTicksLatestPage(AlpacaTicksLatestPage, url=AlpacaStockTicksLatestURL): pass
class AlpacaOptionTicksLatestPage(AlpacaTicksLatestPage, url=AlpacaOptionTicksLatestURL): pass


class AlpacaTicksLatestDownloader(AlpacaDownloader):
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


class AlpacaStockTicksLatestDownloader(AlpacaTicksLatestDownloader, page=AlpacaStockTicksLatestPage, columns=stocks_columns, instrument=Instrument.STOCK):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "ticker"})
        trades = trades.reset_index(drop=True, inplace=False)
        return trades


class AlpacaOptionTicksLatestDownloader(AlpacaTicksLatestDownloader, page=AlpacaOptionTicksLatestPage, columns=options_columns, instrument=Instrument.OPTION):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(trades["osi"].map(OSI).map(asdict), index=trades.index)
        trades = pd.concat([trades, contracts], axis=1)
        trades = trades.reset_index(drop=True, inplace=False)
        return trades




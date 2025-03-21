# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 2025
@name:   Aplaca Portfolio Objects
@author: Jack Kirby Cook

"""
import numpy as np
import pandas as pd

from finance.variables import Variables, Querys, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Partition, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


instrument_parser = lambda string: Variables.Securities.Instrument.OPTION if any(list(map(str.isdigit, string))) else Variables.Securities.Instrument.STOCK
ticker_parser = lambda string: OSI(string).ticker if instrument_parser(string) == Variables.Securities.Instrument.OPTION else str(string).upper()
expire_parser = lambda string: OSI(string).expire if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
option_parser = lambda string: OSI(string).option if instrument_parser(string) == Variables.Securities.Instrument.OPTION else Variables.Securities.Option.EMPTY
strike_parser = lambda string: OSI(string).strike if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
position_parser = lambda string: {"long": Variables.Securities.Position.LONG, "short": Variables.Securities.Position.SHORT}[string]
price_parser = lambda string: np.round(float(string), 2)


class AlpacaPortfolioURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "positions"], headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaPortfolioData(WebJSON, key="portfolio", multiple=True, optional=True):
    class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
    class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
    class Instrument(WebJSON.Text, key="instrument", locator="symbol", parser=instrument_parser): pass
    class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
    class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
    class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
    class Entry(WebJSON.Text, key="entry", locator="avg_entry_price", parser=price_parser): pass
    class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        portfolio = pd.DataFrame.from_records([contents])
        return portfolio


class AlpacaPortfolioPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = AlpacaPortfolioURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaPortfolioData(self.json, *args, **kwargs)
        contents = [data(*args, **kwargs) for data in datas]
        dataframe = pd.concat(contents, axis=0)
        return dataframe


class AlpacaPortfolioDownloader(Logging, Partition, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaPortfolioPage(*args, **kwargs)

    def execute(self, *args, **kwargs):
        portfolio = self.download(*args, **kwargs)
        for settlement, dataframe in self.partition(portfolio, by=Querys.Settlement):
            size = dataframe["quantity"].apply(np.abs).sum()
            self.console(f"{str(settlement)}[{int(size):.0f}]")
        self.stocks(portfolio, *args, **kwargs)
        options = self.options(portfolio, *args, **kwargs)
        yield options

    def download(self, *args, **kwargs):
        portfolio = self.page(*args, **kwargs)
        return portfolio

    @staticmethod
    def stocks(portfolio, *args, **kwargs):
        mask = portfolio["option"].isin([Variables.Securities.Option.EMPTY])
        dataframe = portfolio.where(mask).dropna(how="all", inplace=False)
        function = lambda series: Querys.Symbol(series.to_dict())
        dataframe[str(Querys.Symbol)] = dataframe[list(Querys.Symbol)].apply(function, axis=1)
        dataframe = dataframe.set_index(str(Querys.Symbol), drop=True, inplace=False)
        securities = dataframe[["quantity", "entry"]].to_dict("index")
        return securities

    @staticmethod
    def options(portfolio, *args, **kwargs):
        mask = portfolio["option"].isin([Variables.Securities.Option.PUT, Variables.Securities.Option.CALL])
        dataframe = portfolio.where(mask).dropna(how="all", inplace=False)
        function = lambda series: Querys.Contract(series.to_dict())
        dataframe[str(Querys.Contract)] = dataframe[list(Querys.Contract)].apply(function, axis=1)
        dataframe = dataframe.set_index(str(Querys.Contract), drop=True, inplace=False)
        securities = dataframe[["quantity", "entry"]].to_dict("index")
        return securities

    @property
    def page(self): return self.__page




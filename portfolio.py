# -*- coding: utf-8 -*-
"""
Created on Sun Jul 5 2026
@name:   Alpaca Portfolio Objects
@author: Jack Kirby Cook

"""

import pandas as pd

from finance.enumerations import Instrument, Position
from finance.querys import Contract
from finance.logging import Logging
from finance.osi import OSI
from support.custom import ReversibleDict as RDict
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader", "AlpacaPortfolio"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
position_parser = lambda string: position_mapping[string, True]
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike


AlpacaPortfolio = ["assetID", "ticker", "expire", "option", "strike", "position", "quantity", "entry", "spent"]
class AlpacaPortfolioURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "positions"], headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaPortfolioData(WebJSON, multiple=True, optional=True):
    class AssetID(WebJSON.Text, key="assetID", locator="asset_id", parser=str): pass
    class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
    class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
    class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
    class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
    class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
    class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=int): pass
    class Entry(WebJSON.Text, key="entry", locator="avg_entry_price", parser=float): pass
    class Spent(WebJSON.Text, key="spent", locator="cost_basis", parser=float): pass


class AlpacaPortfolioPage(WebJSONPage):
    def __call__(self, *args, **kwargs):
        url = AlpacaPortfolioURL(authenticator=self.authenticator)
        json = self.load(url)
        datas = AlpacaPortfolioData(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["expire"] = pd.to_datetime(dataframe["expire"])
        dataframe["strike"] = pd.to_numeric(dataframe["strike"])
        return dataframe


class AlpacaPortfolioDownloader(WebStream, Logging, page=AlpacaPortfolioPage):
    def __call__(self, **kwargs):
        portfolio = self.page(**kwargs)
        if bool(portfolio.empty): return pd.DataFrame(columns=AlpacaPortfolio)
        key = lambda series: series.map(str) if series.name == "option" else series
        portfolio = portfolio.sort_values(by=list(Contract), inplace=False, key=key)
        portfolio = portfolio.reset_index(drop=True, inplace=False)
        self.results(portfolio, title="Downloaded", instrument=Instrument.OPTION)
        return portfolio




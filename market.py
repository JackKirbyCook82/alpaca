# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 2025
@name:   Aplaca Market Objects
@author: Jack Kirby Cook

"""

import pytz
import numpy as np
import pandas as pd
from datetime import datetime as Datetime
from collections import namedtuple as ntuple

from finance.variables import Querys, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaContractDownloader", "AlpacaStockDownloader", "AlpacaOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda string: Datetime.fromisoformat(string).astimezone(pytz.timezone("US/Central"))
current_parser = lambda string: np.datetime64(timestamp_parser(string))
ticker_parser = lambda ticker: {"ticker": str(ticker).upper()}
price_parsers = {code: (key, lambda value: np.float32(value)) for key, code in {"price": "p", "ask": "ap", "bid": "bp"}.items()}
size_parsers = {code: (key, lambda value: np.int32(value)) for key, code in {"supply": "as", "demand": "bs"}.items()}
date_parsers = {"t": ("current", lambda value: current_parser(value))}
content_parsers = price_parsers | size_parsers | date_parsers
contents_parser = lambda contents: {key: function(contents[code]) for code, (key, function) in content_parsers.items() if code in contents.keys()}
quote_parser = lambda mapping: [ticker_parser(ticker) | contents_parser(contents) for ticker, contents in mapping.items()]
trade_parser = lambda mapping: [ticker_parser(ticker) | contents_parser(contents) for ticker, contents in mapping.items()]
contracts_parser = lambda mapping: [Querys.Contract(list(osi)) for osi in list(map(OSI, mapping))]


class AlpacaURL(WebURL, domain="https://data.alpaca.markets", headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}

class AlpacaStockURL(AlpacaURL, path=["v2", "stocks"], parameters={"feed": "delayed_sip"}, headers={"accept": "application/json"}):
    @staticmethod
    def parameters(*args, tickers, **kwargs):
        assert isinstance(tickers, list)
        return {"symbols": ",".join(list(tickers))}

class AlpacaOptionURL(AlpacaURL, path=["v1beta1", "options"], parameters={"feed": "indicative"}, headers={"accept": "application/json"}):
    @staticmethod
    def parameters(*args, contracts, **kwargs):
        assert isinstance(contracts, list)
        return {"symbols": ",".join(list(contracts))}


class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass

class AlpacaContractURL(AlpacaURL, path=["v1beta1", "options", "snapshots"], parameters={"feed": "indicative", "limit": 1000}):
    @staticmethod
    def path(*args, ticker, **kwargs): return [str(ticker).upper()]
    @classmethod
    def parameters(cls, *args, **kwargs):
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return expires | strikes | pagination

    @staticmethod
    def expires(*args, expires=None, **kwargs): return {"expiration_date_gte": str(expires.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expires.maximum.strftime("%Y-%m-%d"))} if bool(expires) else {}
    @staticmethod
    def strikes(*args, strikes=None, **kwargs): return {"strike_strike_gte": f"{strikes.minimum}:.02f", "strike_strike_lte": f"{strikes.maximum}:.02f"} if bool(strikes) else {}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if bool(pagination) else {}


class AlpacaData(WebJSON.Mapping, multiple=False, optional=False):
    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, list)
        contents = pd.DataFrame.from_records(contents)
        return contents

class AlpacaStockTradeData(AlpacaData, locator="//trades", parser=trade_parser): pass
class AlpacaStockQuoteData(AlpacaData, locator="//quotes", parser=quote_parser): pass
class AlpacaOptionTradeData(AlpacaData, locator="//trades", parser=trade_parser): pass
class AlpacaOptionQuoteData(AlpacaData, locator="//quotes", parser=quote_parser): pass

class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, locator="next_page_token", key="pagination", parser=str, optional=True): pass
    class Contracts(WebJSON.Mapping, locator="snapshots", key="contracts", parser=contracts_parser):
        def execute(self, *args, **kwargs):
            contracts = super().execute(*args, **kwargs)
            assert isinstance(contracts, list)
            return contracts


class AlpacaStockTradePage(WebJSONPage, url=AlpacaStockTradeURL):
    def execute(self, *args, **kwargs):
        trade = AlpacaStockTradeData(self.json, *args, **kwargs)
        trade = trade(*args, **kwargs)
        return trade

class AlpacaStockQuotePage(WebJSONPage, url=AlpacaStockQuoteURL):
    def execute(self, *args, **kwargs):
        quote = AlpacaStockQuoteData(self.json, *args, **kwargs)
        quote = quote(*args, **kwargs)
        return quote

class AlpacaOptionTradePage(WebJSONPage, url=AlpacaOptionTradeURL):
    def execute(self, *args, **kwargs):
        trade = AlpacaOptionTradeData(self.json, *args, **kwargs)
        trade = trade(*args, **kwargs)
        return trade

class AlpacaOptionQuotePage(WebJSONPage, url=AlpacaOptionQuoteURL):
    def execute(self, *args, **kwargs):
        quote = AlpacaOptionQuoteData(self.json, *args, **kwargs)
        quote = quote(*args, **kwargs)
        return quote

class AlpacaContractPage(WebJSONPage, url=AlpacaContractURL):
    def execute(self, *args, **kwargs):
        pass

#    def execute(self, *args, pagination=None, **kwargs):
#        data = AlpacaContractData(self.json, *args, **kwargs)
#        contracts = data["contracts"](*args, **kwargs)
#        pagination = data["pagination"](*args, **kwargs) if bool(data["pagination"]) else pagination
#        if not bool(pagination): return list(contracts)
#        else: return list(contracts) + self.execute(*args, pagination=pagination, **kwargs)


class AlpacaStockDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pages = ntuple("Pages", "trade quote")
        trade = AlpacaStockTradePage(*args, **kwargs)
        quote = AlpacaStockQuotePage(*args, **kwargs)
        self.__pages = pages(trade, quote)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        if not bool(symbols): return
        stocks = self.download(symbols, *args, **kwargs)
        for symbol, dataframe in self.partition(stocks, by=Querys.Symbol):
            size = self.size(dataframe)
            self.console(f"{str(symbol)}[{int(size):.0f}]")
            if self.empty(dataframe): return
            return dataframe.squeeze()

    def download(self, symbols, *args, **kwargs):
        parameters = dict(tickers=[symbol.ticker for symbol in symbols])
        trade = self.pages.trade(*args, **parameters, **kwargs)
        quote = self.pages.quote(*args, **parameters, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        stocks = trade.merge(quote, how="outer", on=list(Querys.Symbol), sort=False, suffixes=("", "_"))[header]
        return stocks

    @property
    def columns(self): return self.__columns
    @property
    def pages(self): return self.__pages


class AlpacaContractDownloader(Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaContractPage(*args, **kwargs)

    def execute(self, trades, *args, **kwargs):
        assert isinstance(trades, (list, Querys.Trade))
        assert all([isinstance(trade, Querys.Trade) for trade in trades]) if isinstance(trades, list) else True
        trades = list(trades) if isinstance(trades, list) else [trades]
        for trade in iter(trades):
            contracts = self.download(trade, *args, **kwargs)

            print(contracts[0:5])
            raise Exception()

            string = f"{str(trade)}[{len(contracts):.0f}]"
            self.console(string)
            if not bool(contracts): continue
            yield contracts

    def download(self, trade, *args, expires, **kwargs):
        parameters = dict(ticker=trade.ticker, expires=expires, price=trade.price)
        contracts = self.page(*args, **parameters, **kwargs)
        assert isinstance(contracts, list)
        return contracts

    @property
    def page(self): return self.__page


class AlpacaOptionDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pages = ntuple("Pages", "trade quote")
        trade = AlpacaOptionTradePage(*args, **kwargs)
        quote = AlpacaOptionQuotePage(*args, **kwargs)
        self.__pages = pages(trade, quote)

    def execute(self, contracts, *args, **kwargs):
        assert isinstance(contracts, (list, Querys.Contract))
        assert all([isinstance(contract, Querys.Contract) for contract in contracts]) if isinstance(contracts, list) else True
        contracts = list(contracts) if isinstance(contracts, list) else [contracts]
        if not bool(contracts): return
        options = self.download(contracts, *args, **kwargs)
        for settlement, dataframe in self.partition(options, by=Querys.Settlement):
            size = self.size(dataframe)
            self.console(f"{str(settlement)}[{int(size):.0f}]")
            if self.empty(dataframe): return
            return dataframe

    def download(self, contracts, *args, underlying={}, **kwargs):
        parameters = dict(contracts=[contract.toOSI() for contract in contracts])
        trade = self.pages.trade(*args, **parameters, **kwargs)
        quote = self.pages.quote(*args, **parameters, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        options = trade.merge(quote, how="outer", on=list(Querys.Contract), sort=False, suffixes=("", "_"))[header]
        options["underlying"] = options["ticker"].apply(lambda ticker: underlying["ticker"])
        return options

    @property
    def columns(self): return self.__columns
    @property
    def pages(self): return self.__pages




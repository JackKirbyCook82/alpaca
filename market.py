# -*- coding: utf-8 -*-
"""
Created on Thurs Mar 19 2026
@name:   Alpaca Market Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime as Datetime

from finance.enumerations import Instrument, Option
from finance.querys import Symbol, Contract
from finance.logging import Logging
from finance.osi import OSI
from webscraping.webpages import WebJSONPage, WebStream
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockDownloader", "AlpacaContractDownloader", "AlpacaOptionDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


pagination_parser = lambda string: str(string) if string != "None" else None
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
strike_parser = lambda string: np.round(float(string), 2)


class AlpacaMarketURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaSecurityURL(AlpacaMarketURL): pass
class AlpacaStockURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"feed": "delayed_sip"}):
    @staticmethod
    def parameters(*args, tickers, **kwargs):
        return {"symbols": ",".join(list(tickers))}

class AlpacaOptionURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v1beta1", "options"], parameters={"feed": "indicative"}):
    @staticmethod
    def parameters(*args, osis, **kwargs):
        return {"symbols": ",".join(list(osis))}


class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass


class AlpacaContractURL(AlpacaMarketURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return tickers | expires | pagination

    @staticmethod
    def tickers(*args, ticker, **kwargs):
        return {"underlying_symbols": str(ticker)}

    @staticmethod
    def expires(*args, expires=None, **kwargs):
        if expires is not None: return {"expiration_date_gte": str(expires.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expires.maximum.strftime("%Y-%m-%d"))}
        else: return {}

    @staticmethod
    def strikes(*args, strikes=None, **kwargs):
        if strikes is not None: return {"strike_price_gte": str(strikes.minimum), "strike_price_lte": str(strikes.maximum)}
        else: return {}

    @staticmethod
    def pagination(*args, pagination=None, **kwargs):
        if pagination is not None: return {"page_token": str(pagination)}
        else: return {}


class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="//option_contracts[]", parser=Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="//underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="//expiration_date", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="//type", parser=Option): pass
        class Strike(WebJSON.Text, key="strike", locator="//strike_price", parser=strike_parser): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaMarketPage(WebJSONPage, ABC): pass
class AlpacaSecurityPage(AlpacaMarketPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("last", "p", np.float32), AlpacaField("bid", "bp", np.float32), AlpacaField("ask", "ap", np.float32), AlpacaField("supply", "as", np.float32), AlpacaField("demand", "bs", np.float32)]
        parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in fields if field.code in mapping.keys()}
        merger = lambda quotes, trades, on: quotes.merge(trades, on=on, how="left", validate="one_to_one")
        self.__merger = merger
        self.__fields = fields
        self.__parser = parser

    @abstractmethod
    def trades(self, *args, **kwargs): pass
    @abstractmethod
    def quotes(self, *args, **kwargs): pass

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser
    @property
    def merger(self): return self.__merger


class AlpacaStockPage(AlpacaSecurityPage):
    def __call__(self, *args, tickers, **kwargs):
        assert isinstance(tickers, list)
        tickers = list(map(str, tickers))
        parameters = dict(tickers=tickers, authenticator=self.authenticator)
        trades = self.trades(**parameters)
        quotes = self.quotes(**parameters)
        stocks = self.merger(quotes, trades, on="ticker")
        stocks["expire"] = pd.to_datetime(stocks["expire"])
        stocks["strike"] = pd.to_numeric(stocks["strike"])
        return stocks

    def trades(self, *args, **kwargs):
        url = AlpacaStockTradeURL(*args, **kwargs)
        json = self.load(url)["trades"]
        records = [{"ticker": ticker} | self.parser(mapping) for ticker, mapping in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        return dataframe

    def quotes(self, *args, **kwargs):
        url = AlpacaStockQuoteURL(*args, **kwargs)
        downloaded = self.load(url)["quotes"]
        json = [{"ticker": ticker} | self.parser(mapping) for ticker, mapping in downloaded.items()]
        dataframe = pd.DataFrame.from_records(json)
        return dataframe


class AlpacaContractPage(AlpacaMarketPage):
    def __call__(self, *args, ticker, expires=None, strikes=None, **kwargs):
        parameters = dict(ticker=ticker, expires=expires, strikes=strikes, authenticator=self.authenticator)
        contracts = self.contracts(**parameters)
        return contracts

    def contracts(self, *args, pagination=None, **kwargs):
        url = AlpacaContractURL(*args, pagination=pagination, **kwargs)
        json = self.load(url)
        datas = AlpacaContractData(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.contracts(*args, pagination=pagination, **kwargs)


class AlpacaOptionPage(AlpacaSecurityPage):
    def __call__(self, *args, contracts, **kwargs):
        osis = list(map(lambda contract: str(OSI(contract)), contracts))
        parameters = dict(osis=osis, authenticator=self.authenticator)
        trades = self.trades(**parameters)
        quotes = self.quotes(**parameters)
        options = self.merger(quotes, trades, on="osi")
        options["expire"] = pd.to_datetime(options["expire"])
        options["strike"] = pd.to_numeric(options["strike"])
        return options

    def trades(self, *args, **kwargs):
        url = AlpacaOptionTradeURL(*args, **kwargs)
        json = self.load(url)["trades"]
        records = [{"osi": osi} | self.parser(mapping) for osi, mapping in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        return dataframe

    def quotes(self, *args, **kwargs):
        url = AlpacaOptionQuoteURL(*args, **kwargs)
        json = self.load(url)["quotes"]
        records = [{"osi": osi} | self.parser(mapping) for osi, mapping in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        return dataframe


class AlpacaMarketDownloader(WebStream, Logging, ABC):
    @abstractmethod
    def downloader(self, *args, **kwargs): pass


class AlpacaStockDownloader(AlpacaMarketDownloader, page=AlpacaStockPage):
    def __call__(self, symbols, /, **kwargs):
        assert isinstance(symbols, (list, Symbol))
        assert all([isinstance(symbol, Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        if not isinstance(symbols, list): symbols = [symbols]
        tickers = [symbol.ticker for symbol in list(dict.fromkeys(symbols))]
        stocks = self.downloader(tickers, **kwargs)
        stocks = pd.concat(list(stocks), axis=0)
        stocks = stocks.sort_values(by=list(Symbol), inplace=False)
        stocks = stocks.reset_index(drop=True, inplace=False)
        return stocks

    def downloader(self, tickers, /, **kwargs):
        tickers = [tickers[index:index+self.capacity] for index in range(0, len(tickers), self.capacity)]
        for tickers in tickers:
            stocks = self.page(tickers=tickers, **kwargs)
            if bool(stocks.empty): continue
            self.results(stocks, title="Downloaded", instrument=Instrument.STOCK)
            yield stocks


class AlpacaContractDownloader(AlpacaMarketDownloader, page=AlpacaContractPage):
    def __call__(self, symbols, /, **kwargs):
        assert isinstance(symbols, (list, Symbol))
        assert all([isinstance(symbol, Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        if not isinstance(symbols, list): symbols = [symbols]
        tickers = [symbol.ticker for symbol in list(dict.fromkeys(symbols))]
        contracts = self.downloader(tickers, **kwargs)
        contracts = list(contracts)
        contracts.sort(key=lambda contract: (contract.ticker, contract.expire))
        return contracts

    def downloader(self, tickers, /, **kwargs):
        for ticker in tickers:
            contracts = self.page(ticker=ticker, **kwargs)
            self.results(contracts, title="Downloaded", instrument=Instrument.CONTRACT)
            for contract in contracts: yield contract


class AlpacaOptionDownloader(AlpacaMarketDownloader, page=AlpacaOptionPage):
    def __call__(self, contracts, /, **kwargs):
        assert isinstance(contracts, (list, Contract))
        assert all([isinstance(contract, Contract) for contract in contracts]) if isinstance(contracts, list) else True
        if not isinstance(contracts, list): contracts = [contracts]
        contracts = list(dict.fromkeys(contracts))
        options = self.downloader(contracts, **kwargs)
        options = pd.concat(list(options), axis=0)
        key = lambda series: series.map(str) if series.name == "option" else series
        options = options.sort_values(by=list(Contract), inplace=False, key=key)
        options = options.reset_index(drop=True, inplace=False)
        return options

    def downloader(self, contracts, /, **kwargs):
        contracts = [contracts[index:index+self.capacity] for index in range(0, len(contracts), self.capacity)]
        for contracts in contracts:
            options = self.page(contracts=contracts, **kwargs)
            if bool(options.empty): continue
            contracts = options["osi"].apply(lambda osi: asdict(osi), axis=1, result_type="expand")
            options = pd.concat([options, contracts], axis=1).drop(columns=["osi"], inplace=False)
            self.results(options, title="Downloaded", instrument=Instrument.OPTION)
            yield options

    @staticmethod
    def unpack(options):
        series = options.pop("osi").apply(OSI)
        options["ticker"] = series.apply(lambda osi: osi.ticker)
        options["expire"] = series.apply(lambda osi: osi.expire)
        options["option"] = series.apply(lambda osi: osi.option)
        options["strike"] = series.apply(lambda osi: osi.strike)
        return options

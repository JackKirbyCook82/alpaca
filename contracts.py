# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Contract Objects
@author: Jack Kirby Cook
@file:   alpaca/contracts.py

"""

import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime as Datetime

from finance.enumerations import Instrument, Option
from finance.querys import Symbol, Contract
from finance.reporting import Results
from finance.osi import OSI
from webscraping.webpages import WebJSONPage, WebStream
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


pagination_parser = lambda string: str(string) if string != "None" else None
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
strike_parser = lambda string: np.round(float(string), 2)


class AlpacaContractURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}, headers={"accept": "application/json"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        products = cls.products(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return products | expires | strikes | pagination

    @staticmethod
    def products(*args, products, **kwargs):
        return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

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

    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="//option_contracts[]", parser=Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="//underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="//expiration_date", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="//type", parser=Option): pass
        class Strike(WebJSON.Text, key="strike", locator="//strike_price", parser=strike_parser): pass


class AlpacaContractPage(WebJSONPage, ABC):
    def __call__(self, *args, ticker, expires=None, strikes=None, **kwargs):
        parameters = dict(ticker=ticker, expires=expires, strikes=strikes, authenticator=self.authenticator)
        contracts = self.execute(**parameters)
        return contracts

    def execute(self, *args, pagination=None, **kwargs):
        url = AlpacaContractURL(*args, pagination=pagination, **kwargs)
        json = self.load(url)
        datas = AlpacaContractData(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.execute(*args, pagination=pagination, **kwargs)


class AlpacaContractDownloader(WebStream, Results, Logging, ABC, page=AlpacaContractPage):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        contracts = self.downloader(products, **kwargs)
        contracts = list(contracts)
        contracts.sort(key=lambda contract: (contract.ticker, contract.expire))
        return contracts

    def downloader(self, products, /, **kwargs):
        for product in products:
            scope = self.scope([product], instrument=Instrument.STOCK)
            contracts = self.page(products=products, **kwargs)
            results = self.results(scope=scope, size=len(contracts))
            self.console("Downloaded", results)
            for contract in contracts: yield contract

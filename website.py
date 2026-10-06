# -*- coding: utf-8 -*-
"""
Created on Mon Oct 5 2026
@name:   Alpaca Website Objects
@author: Jack Kirby Cook
@file:   alpaca\website.py

"""

from abc import ABC
from types import SimpleNamespace
from dataclasses import dataclass

from finance.reporting import Results
from webscraping.weburl import WebURL
from webscraping.webpages import WebJSONPage
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaDownloadPage", "AlpacaUploadPage", "AlpacaDownloadURL", "AlpacaUploadURL", "AlpacaDownloader", "AlpacaField"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaURL(WebURL):
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaUploadURL(AlpacaURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
class AlpacaDownloadURL(AlpacaURL, headers={"accept": "application/json"}): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaPage(WebJSONPage):
    def __init_subclass__(cls, /, url=None, data=None, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = SimpleNamespace(url=url, data=data)
        cls.__namespace__ = namespace

    def __init__(self, *args, authenticator, **kwargs):
        super().__init__(*args, **kwargs)
        self.__authenticator = authenticator

    def url(self, *args, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        return self.namespace.url(*args, **parameters, **kwargs)

    def data(self, json, *args, **kwargs):
        if self.namespace.data is None: return None
        return self.namespace.data(json, *args, **kwargs)

    @property
    def namespace(self): return type(self).__namespace__
    @property
    def authenticator(self): return self.__authenticator


class AlpacaUploadPage(AlpacaPage): pass
class AlpacaDownloadPage(AlpacaPage): pass


class AlpacaDownloader(Results, Logging, ABC):
    def __init_subclass__(cls, /, page=None, columns=None, instrument=None, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = dict(pages=kwargs.get("pages", {}), page=page, columns=columns, instrument=instrument)
        cls.__namespace__ = SimpleNamespace(**namespace)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        namespace = type(self).__namespace__
        self.__pages = {key: page(*args, **kwargs) for key, page in namespace.pages.items()}
        self.__page = namespace.page(*args, **kwargs) if namespace.page is not None else None
        self.__instrument = namespace.instrument
        self.__columns = namespace.columns

    def scope(self, products, **kwargs):
        if self.instrument is None: raise ValueError(self.instrument)
        return super().scope(products, instrument=self.instrument)

    @property
    def instrument(self): return self.__instrument
    @property
    def columns(self): return self.__columns
    @property
    def pages(self): return self.__pages
    @property
    def page(self): return self.__page




#   PyCurator LGPL 3.0 <https://www.gnu.org/licenses/lgpl-3.0.txt>
#   Copyright (c) 2022. Michael Baluja

from collections.abc import Callable, Iterable
from re import Pattern
from typing import Any, TypeVar, Union

from bs4.element import Tag
from pandas import DataFrame

Page = TypeVar('Page', bound='gui.ViewPage')

SearchTerm = TypeVar('SearchTerm', bound=str)
SearchType = TypeVar('SearchType', bound=str)
SearchTuple = tuple[SearchTerm, SearchType]
SearchQuery = Union[SearchType, SearchTerm, SearchTuple]

SearchResult = Union[DataFrame, None]
TermResultDict = dict[SearchTerm, SearchResult]
TypeResultDict = dict[SearchType, SearchResult]
TermTypeResultDict = dict[SearchTuple, SearchResult]
QueryResultDict = dict[SearchQuery, SearchResult]

JSONDict = dict[str, Any]

AttributeKey = TypeVar('AttributeKey', bound=str)
AttributeValue = TypeVar('AttributeValue', bound=str)
AttributeDict = dict[AttributeKey, AttributeValue]

SimpleStrainable = Union[
    str,
    bool,
    None,
    bytes,
    Pattern[str],
    Callable[[str], bool],
    Callable[[Tag], bool]
]
Strainable = Union[SimpleStrainable, Iterable[SimpleStrainable]]

TKVarValue = TypeVar('TKVarValue', str, int, float, bool)

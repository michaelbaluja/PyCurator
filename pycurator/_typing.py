from typing import Any, TypeVar, Union

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

TKVarValue = TypeVar('TKVarValue', str, int, float, bool)

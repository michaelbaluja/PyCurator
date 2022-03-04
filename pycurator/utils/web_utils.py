import bs4
import re
from typing import Any, Optional, ParamSpec, Union
from pycurator.utils.typing import Strainable


P = ParamSpec('P')


def get_single_tag_from_path(
        soup: bs4.BeautifulSoup,
        path: str
) -> Union[bs4.element.Tag, None]:
    """Extract HTML given a CSS path."""
    return soup.select_one(path)


def get_single_tag_from_tag_info(
        soup: bs4.BeautifulSoup,
        class_type: Strainable = re.compile(r''),
        **kwargs: Any
) -> Union[bs4.element.Tag, bs4.element.NavigableString, None]:
    """Find and return BeautifulSoup Tag from given specifications."""
    return soup.find(class_type, **kwargs)


def get_parent_tag(
        soup: bs4.BeautifulSoup,
        string: Strainable,
) -> Union[bs4.element.Tag, None]:
    """Find BeautifulSoup Tag from given specifications, return parent."""
    attr = get_single_tag_from_tag_info(
        soup=soup,
        string=string
    )
    try:
        parent_tag = attr.parent
    except AttributeError:
        parent_tag = None

    return parent_tag


def get_sibling_tag(
        soup: bs4.BeautifulSoup,
        string: Strainable,
        pattern: Strainable = re.compile(r''),
        *args: P.args,
        **kwargs: P.kwargs
) -> Union[bs4.element.Tag, bs4.element.NavigableString, None]:
    tag = get_single_tag_from_tag_info(
        soup=soup,
        class_type=pattern,
        string=string
    )
    return tag.find_next_sibling(*args, **kwargs)


def get_sibling_tags(
        soup: bs4.BeautifulSoup,
        string: Strainable,
        *args: Any,
        **kwargs: Any
) -> bs4.element.ResultSet[bs4.element.PageElement]:
    """Return the sibling tags.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
    string : str
        Pattern for locating tag of interest.
    **kwargs : dict, optional
        Additional parameters passed to the
        bs4.element.Tag.find_next_siblings() call.

    Returns
    -------
    list of bs4.element.Tag or empty

    See Also
    --------
    bs4.element.Tag.find_next_siblings
    """

    tag = get_single_tag_from_tag_info(
        soup=soup,
        string=string
    )
    return tag.find_next_siblings(*args, **kwargs)


def get_parent_sibling_tags(
        soup: bs4.BeautifulSoup,
        string: Strainable,
        **kwargs: Any
) -> bs4.element.ResultSet[bs4.element.PageElement]:
    """Return the tag for the parent tag's sibling tags.

    Parameters
    ----------
    soup : bs4.BeautifulSoup
    string : str
        Pattern for locating tag of interest.
    **kwargs : dict, optional
        Additional parameters passed to the
        bs4.element.Tag.find_next_siblings() call.

    Returns
    -------
    list of bs4.element.Tag or empty

    See Also
    --------
    bs4.element.Tag.find_next_siblings
    """

    parent = get_parent_tag(soup, string)
    return parent.find_next_siblings(**kwargs)


def get_tag_value(
        tag: bs4.element.PageElement,
        err_return: Optional[Any] = None,
        **kwargs: Any
) -> str or Any:
    """Return text for the provided Tag, queried with kwargs."""
    try:
        return tag.get_text(**kwargs)
    except AttributeError:
        return err_return


def get_single_tag(
        soup: bs4.BeautifulSoup,
        path: Optional[str] = None,
        class_type: Strainable = re.compile(r''),
        **find_kwargs: Any
) -> Union[bs4.element.Tag, bs4.element.NavigableString, None]:
    """Retrieves the requested value from the soup object.

    For a page attribute with a single value
    ('abstract', 'num_instances', etc.), returns the value.

    Either a full CSS Selector Path must be passed via 'path', or an HTML
    class and additional parameters must be passed via 'class_type' and
    **find_kwargs, respectively.

    For attributes with potentially multiple values, such as 'keywords',
    use get_variable_attribute_values(...)

    Parameters
    ----------
    soup : BeautifulSoup
        HTML to be parsed.
    path : str, optional (default=None)
        CSS Selector Path for attribute to scrape.
        If None:
            Search is performed using class_type and **find_kwargs.
    class_type : str, optional (default=re.compile(r''))
        HTML class type to find.
    **find_kwargs : dict, optional
        Additional arguments for 'soup.find()' call.

    Returns
    -------
    attr : bs4.element.Tag or None

    Raises
    ------
    ValueError
        If no CSS path or find_kwargs are passed.

    See Also
    --------
    re.compile : Compile a regular expression pattern into a regular
        expression object, which can be used for matching using
        re.search.
    """

    if path:
        attr = get_single_tag_from_path(soup, path)
    elif find_kwargs:
        attr = get_single_tag_from_tag_info(
            soup,
            class_type,
            **find_kwargs
        )
    else:
        raise ValueError('Must pass a CSS path or find attributes.')

    return attr


def get_variable_tags(
        soup: bs4.BeautifulSoup,
        path: Optional[str] = None,
        class_type: Strainable = re.compile(r''),
        **find_kwargs: Any
) -> bs4.element.ResultSet:
    """Retrieves the requested value from the soup object.

    For a page attribute with potentially multiple values, such as
    'keywords', return the values as a list. For attributes with a single
    value, such as 'abstract', use get_single_attribute_value(...)

    Parameters
    ----------
    soup : BeautifulSoup
        HTML to be parsed.
    path : str, optional (default=None)
        CSS Selector Path for attribute to scrape.
    class_type : str, optional (default=re.compile(r''))
        HTML class type to find.
    **find_kwargs : dict, optional
        Additional arguments for 'soup.find_all()' call.

    Returns
    -------
    attrs : list of bs4.element.Tag or None

    Raises
    ------
    ValueError
        If no CSS path or find_kwargs are passed.
    """

    if path:
        attrs = soup.select(path)
    elif find_kwargs:
        attrs = soup.find_all(class_type, **find_kwargs)
    else:
        raise ValueError('Must pass a CSS path or find attributes.')

    return attrs

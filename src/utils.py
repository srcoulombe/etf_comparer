# utils.py

# standard library dependencies
from typing import Mapping, List, Tuple, Callable, Union, Iterable, Any

# external dependencies
import pandas as pd
from scipy.spatial.distance import jaccard

# TODO: tests
def get_boundaries(enumerable: Iterable[Any]) -> List[int]:
    """Returns the indices of items in `enumerable`
    which differ from the previous item. Also includes the
    0th and `len(enumerables)`th index.

    Parameters
    ----------
    enumerable : Iterable[Any]
        Some iterable whose items can be compared using
        `==` and `!=`.

    Returns
    -------
    List[int]
        List of integers representing the indices in `enumerable`
        where `enumerable[i] != enumerable[i+1]` (as well as indices for 
        `0` and `len(enumerable)`)
    
    Examples
    --------
    >>> test = get_boundaries([0])
    >>> assert test == [0, 1]
    >>> test = get_boundaries([0,0])
    >>> assert test == [0, 2]
    >>> test = get_boundaries([0, 1])
    >>> assert test == [0, 1, 2]
    >>> test = get_boundaries([0, 0, 1, 1, 0, 1, 0, 0])
    >>> assert test == [0, 2, 4, 5, 6, 8]
    """
    transitions = [ i+1 for i, (enumerable_at_i, enumerable_at_i_plus_one)
                    in enumerate(zip(enumerable[:-1], enumerable[1:]))
                    if enumerable_at_i != enumerable_at_i_plus_one ]
    return [0] + transitions + [len(enumerable)]

# TODO: tests
def get_contiguous_truthy_segments(enumerable_: List[bool]) -> List[Tuple[int,int]]:
    """Returns a list of tuples of integers indicating the `(inclusive_starting_index, exclusive_stopping_index)`
    of contiguous truthy segments in `enumerable` (the List[bool] version of `enumerable_`).

    Parameters
    ----------
    enumerable_ : List[bool]
        Iterable to process. Gets automatically converted to a List[bool] (not in-place).

    Returns
    -------
    List[Tuple[int,int]]
        List of tuples of integers indicating the `(inclusive_starting_index, exclusive_stopping_index)`
        of contiguous truthy segments in `enumerable` (the List[bool] version of `enumerable_`).
    
    Examples
    --------
    >>> out = get_contiguous_truthy_segments([])
    >>> assert out == []
    >>> out = get_contiguous_truthy_segments([1])
    >>> assert out == [(0,1)]
    >>> out = get_contiguous_truthy_segments([1,1])
    >>> assert out == [(0,2)]
    >>> out = get_contiguous_truthy_segments([1,1,0])
    >>> assert out == [(0,2)]
    >>> out = get_contiguous_truthy_segments([0,1,0,1,1,1])
    >>> assert out == [(1,2),(3,6)]
    >>> out = get_contiguous_truthy_segments([False, False,])
    >>> assert out == []
    """
    enumerable = list(map(bool, enumerable_))
    contigs: List[Tuple[int,int]] = []
    inclusive_starting_index = 0
    while inclusive_starting_index < len(enumerable):
        if enumerable[inclusive_starting_index] is False: 
            inclusive_starting_index += 1
        else:
            stop_exc = inclusive_starting_index
            while stop_exc < len(enumerable) and enumerable[stop_exc]:
                stop_exc += 1
            contigs.append((inclusive_starting_index, stop_exc))
            inclusive_starting_index = stop_exc
    return contigs

def get_all_holdings(query_output: Mapping[str, Mapping[str, Mapping]]) -> List[str]:
    """Convenience function used to get all the holding tickers
    (strings) from the provided `query_output` dictionary.

    Parameters
    ----------
    query_output : Mapping[str, Mapping[str, Mapping]]
        Dictionary mapping an ETF ticker (strings) to a sub-dictionary
        mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
        w.r.t. the ETF).
        See the documentation for the `get_holdings_and_weights_for_etfs` method from
        `src.dbms.SQLDatabaseClient` or `src.dbms.TinyDBDatabaseClient`.

    Returns
    -------
    List[str]
        A list of all holdings in `query_output`.
    
    Notes
    -----
    The returned list does not contain any duplicates. 

    Examples
    --------
    >>> sample = {"etf1": {"tickerA": 0.5, "tickerB": 0.5}, "etf2": {"tickerC": 1.0}}
    >>> out = get_all_holdings(sample)
    >>> assert out == ['tickerA', 'tickerB', 'tickerC']
    """
    return sorted(set(
        [
            holding 
            for etf, etf_holdings_dict in query_output.items()
            for holding in etf_holdings_dict.keys()
        ]
    ))

# TODO: tests
def get_etf_holding_weight_vectors( query_output: Mapping[str, Mapping[str, Mapping]],
                                    all_holdings: List[str] = None,
                                    as_df: bool = False) -> Union[pd.DataFrame, Mapping[str, List[float]]]:
    """Converts the `query_output` dictionary to a dictionary mapping an ETF ticker (string)
    to a list of floats indicating the weight of all relevant holdings for that ETF.

    Parameters
    ----------
    query_output : Mapping[str, Mapping[str, Mapping]]
        Dictionary mapping an ETF ticker (strings) to a sub-dictionary
        mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
        w.r.t. the ETF).
        See the documentation for the `get_holdings_and_weights_for_etfs` method from
        `src.dbms.SQLDatabaseClient` or `src.dbms.TinyDBDatabaseClient`.
    all_holdings : List[str], optional
        Optional list of all holdings to consider, by default None.
        If kept as None, it gets converted to the output of `reorder_holdings_by_popularity`

    Returns
    -------
    Union[pd.DataFrame, Mapping[str, List[float]]]
        Dictionary mapping an ETF's ticker (string) to a list of floats. 
        Each position in the list of floats corresponds to a holding held by >= 1
        ETF in `query_output`. The float at position `i` indicates the weight of the
        `ith` holding in the corresponding ETF.
        Can also be a Pandas DataFrame with holdings as the index and etfs as columns.

    Examples
    --------
    >>> example_query_output = {'etf1': {'A': {'weight': 0.2}, 'B': {'weight': 0.3}}, 'etf2': {'C': {'weight': 1.0}}}
    >>> out = get_etf_holding_weight_vectors(example_query_output)
    >>> assert out == {"etf1": [0.2, 0.3, 0.0], "etf2": [0.0, 0.0, 1.0]}
    >>> out = get_etf_holding_weight_vectors(example_query_output, as_df=True)
    >>> expected = pd.DataFrame({"etf1": [0.2, 0.3, 0.0], "etf2": [0.0, 0.0, 1.0]}, index=['A','B','C'])
    >>> assert expected.equals(out)
    >>> assert get_etf_holding_weight_vectors(dict()) == dict()
    """
    if all_holdings is None:
        all_holdings = [holding for holding, annotation in 
                        reorder_holdings_by_popularity(query_output)]

    etfs_as_vectors = {
        etf: [0.0]*len(all_holdings)
        for etf in query_output.keys()
    }
    for etf, etf_holdings_dict in query_output.items():
        for i, holding in enumerate(all_holdings):
            try:
                etf_holding_percentage = etf_holdings_dict[holding]['weight']
            except KeyError:
                etf_holding_percentage = 0.0
            etfs_as_vectors[etf][i] = etf_holding_percentage
    if as_df:
        df = pd.DataFrame.from_dict(etfs_as_vectors)
        df.index = all_holdings
        return df
    return etfs_as_vectors

# TODO: tests
def weighted_jaccard_distance(vector1: Iterable[float], vector2: Iterable[float]) -> float:
    """Convenience function implementing the weighted Jaccard Distance metric.

    Parameters
    ----------
    vector1 : Iterable[float]
        An iterable of floats.
        Presumed to be sorted in the same order as `vector2`.
    vector2 : Iterable[float]
        An iterable of floats.
        Presumed to be sorted in the same order as `vector1`.

    Returns
    -------
    float
        The weighted Jaccard Distance between `vector1` and `vector2`

    References
    ----------
    - https://en.wikipedia.org/wiki/Jaccard_index#Weighted_Jaccard_similarity_and_distance

    Notes
    -----
    The `w` kwarg to `scipy.distance.jaccard` does not compute the weighted jaccard distance
    as it is described in the reference above.
    
    Examples
    --------
    >>> vector1 = [1, 0, 0]
    >>> vector2 = [0, 1, 0]
    >>> assert weighted_jaccard_distance(vector1, vector2) == 1
    >>> assert weighted_jaccard_distance(vector2, vector1) == 1
    >>> assert weighted_jaccard_distance(vector1, vector1) == 0
    >>> assert weighted_jaccard_distance(vector2, vector2) == 0
    >>> vector1 = [2, 0, 0]
    >>> vector2 = [2, 0, 3]
    >>> assert weighted_jaccard_distance(vector1, vector2) == 0.6
    >>> vector1 = [2, 1, 0]
    >>> vector2 = [2, 1, 3]
    >>> assert weighted_jaccard_distance(vector1, vector2) == 0.5
    >>> vector1 = [.2, .1, .0]
    >>> vector2 = [.2, .1, .3]
    >>> assert weighted_jaccard_distance(vector1, vector2) == 0.5
    >>> assert weighted_jaccard_distance([0,0,0,0], [0,0,0,0]) == 0
    """
    v1 = list(vector1)
    v2 = list(vector2)
    assert len(v1) == len(v2), \
        "`weighted_jaccard_distance` is meant to be applied to vectors of equal lengths."
    assert min(min(v1), min(v2)) >= 0, \
        "`weighted_jaccard_distance` is meant to be used on vectors with values >= 0.0."
    numerator = sum(
        [min(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)]
    )
    denominator = sum(
        [max(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)]
    )
    try:
        weighted_jaccard_similarity = numerator / denominator
    except ZeroDivisionError:
        # can only happen if v1 and v2 only contain 0s,
        # in which case the vectors are identical
        weighted_jaccard_similarity = 1
    # `numerator \ denominator` returns 
    return 1.0 - weighted_jaccard_similarity

# TODO: tests
def get_similarity( query_output: Mapping[str, Mapping[str, Mapping]],
                    distance_measure: Union[str,Callable] = jaccard) -> Mapping[Tuple[str,str], float]:
    """Wrapper around the functions for the supported distance measures 
    (Jaccard Distance, and weighted Jaccard Distance).

    Parameters
    ----------
    query_output : Mapping[str, Mapping[str, Mapping]]
        Dictionary mapping an ETF ticker (strings) to a sub-dictionary
        mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
        w.r.t. the ETF).
        See the documentation for the `get_holdings_and_weights_for_etfs` method from
        `src.dbms.SQLDatabaseClient` or `src.dbms.TinyDBDatabaseClient`.
    distance_measure : Union[str,Callable], optional
        Either the string indicating which distance metric to use 
        (must be one of 'jaccard','weighted_jaccard'), or the function
        itself. 
        By default 'jaccard'

    Returns
    -------
    Mapping[Tuple[str,str], float]
        Dictionary mapping a tuple of strings (two ETF tickers) to their distance
        (according to the chosen metric).
    
    Examples
    --------
    >>> sample = {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}}
    >>> assert get_similarity(sample, distance_measure="jaccard") == {("etf1", "etf2"): 0.0}
    >>> assert get_similarity(sample, distance_measure="weighted_jaccard") == {("etf1", "etf2"): 0.0}
    >>> sample = sample = {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerA": {"weight": 0.1}, "tickerD": {"weight": 0.3}, "tickerE": {"weight": 0.3}, "tickerF": {"weight": 0.3}}}
    >>> similarities = get_similarity(sample, distance_measure="jaccard")
    >>> assert round(similarities[('etf1','etf2')],3) == 0.2
    >>> similarities = get_similarity(sample, distance_measure="weighted_jaccard")
    >>> assert round(similarities[('etf1','etf2')],3) == 0.053
    """
    if isinstance(distance_measure, str):
        distance_measure = distance_measure.lower()
        assert distance_measure in ('jaccard','weighted_jaccard'), \
            f"{distance_measure} is not among the supported distance measures ('jaccard', 'weighted_jaccard')"
        distance_measure_to_function = {
            'jaccard': jaccard,
            'weighted_jaccard': weighted_jaccard_distance
        }
        distance_measure = distance_measure_to_function[distance_measure]
        
    assert distance_measure in (jaccard, weighted_jaccard_distance)
    etfs_as_vectors = get_etf_holding_weight_vectors(
        query_output
    )
    
    etf_names = sorted(etfs_as_vectors.keys())
    similarities = dict()
    for i, etf1_name in enumerate(etf_names):
        for etf2_name in etf_names[i+1:]:
            if distance_measure is jaccard:
                # need to map to booleans
                similarities[(etf1_name, etf2_name)] = 1.0 - distance_measure(
                    list(map(bool, etfs_as_vectors[etf1_name])),
                    list(map(bool, etfs_as_vectors[etf2_name]))
                )
            else:
                similarities[(etf1_name, etf2_name)] = 1.0 - distance_measure(
                    etfs_as_vectors[etf1_name],
                    etfs_as_vectors[etf2_name]
                )
    return similarities

# TODO: tests
def annotate_holdings(query_output: Mapping[str, Mapping[str, Mapping]]) -> Mapping[str, str]:
    """Returns a dictionary mapping each holding held by >= 1 ETF in `query_output`
    to a string of length = `len(query_output)`. These strings (annotations) are comprised of 
    `1`s and `0`s such that `annotations[i] == '1'` if the corresponding holding was held by
    ETF `i` and `'0'` otherwise.

    Parameters
    ----------
    query_output : Mapping[str, Mapping[str, Mapping]]
        Dictionary mapping an ETF ticker (strings) to a sub-dictionary
        mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
        w.r.t. the ETF).
        See the documentation for the `get_holdings_and_weights_for_etfs` method from
        `src.dbms.SQLDatabaseClient` or `src.dbms.TinyDBDatabaseClient`.

    Returns
    -------
    Mapping[str, str]
        A dictionary mapping a holding ticker (string) to an annotation string
        whose `ith` character indicates whether the `ith` ETF held the corresponding holding
        (`'1'`) or not (`'0'`).
    
    Examples
    --------
    >>> sample = {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}}
    >>> out = annotate_holdings(sample)
    >>> assert out == {"tickerA": '10', "tickerB": '10', "tickerC": '01'}
    >>> sample = {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerA": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerD": {"weight": 0.05}}}
    >>> out = annotate_holdings(sample)
    >>> assert out == {'tickerA': '101', 'tickerB': '101', 'tickerC': '010', 'tickerD': '001'}

    """
    all_holdings_with_annotations: Mapping[str, tuple] = dict()
    # loop over all ETFs
    for i, (etf, etf_holdings_dict) in enumerate(query_output.items()):
        # loop over each holding in the current ETF
        for holding in etf_holdings_dict.keys():
            current_annotation = all_holdings_with_annotations.get(
                holding,
                [0]*len(query_output)
            )
            current_annotation[i] = 1
            all_holdings_with_annotations[holding] = current_annotation
    return {
        key: ''.join(map(str, annotation))
        for key, annotation in 
        all_holdings_with_annotations.items()
    }

# TODO: tests
def reorder_holdings_by_popularity(query_output: Mapping[str, Mapping[str, Mapping]]) -> List[Tuple[str,str]]:
    """Convenience wrapper around `annotate_holdings` that sorts its result such that
    the holding tickers (strings) shared among the most ETFs are first in the sequence.
    
    Parameters
    ----------
    query_output : Mapping[str, Mapping[str, Mapping]]
        Dictionary mapping an ETF ticker (strings) to a sub-dictionary
        mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
        w.r.t. the ETF).
        See the documentation for the `get_holdings_and_weights_for_etfs` method from
        `src.dbms.SQLDatabaseClient` or `src.dbms.TinyDBDatabaseClient`.

    Returns
    -------
    List[Tuple[str,str]]
        List of (ticker, etf_membership) key:value pairs constructed by
        sorting the output of `annotate_holdings(query_output)` such that the
        tickers are listed in decreasing popularity.
        
    
    Examples
    --------
    >>> sample = {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerA": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerD": {"weight": 0.05}}}
    >>> out = annotate_holdings(sample)
    >>> assert out == {'tickerA': '101', 'tickerB': '101', 'tickerC': '010', 'tickerD': '001'}
    >>> out = reorder_holdings_by_popularity(sample)
    >>> assert out == [('tickerA', '101'), ('tickerB', '101'), ('tickerC', '010'), ('tickerD', '001')]
    """
    return sorted(
        annotate_holdings(query_output).items(),
        key = lambda item: int(item[1],2),
        reverse = True
    )

if __name__ == '__main__':
    import doctest 
    doctest.testmod()
# utils.py

# standard library dependencies
from typing import Mapping, List, Tuple, Callable, Union, Iterable, Any

# external dependencies
import pandas as pd
from scipy.spatial.distance import cosine, jaccard

# TODO: summary
def get_boundaries(enumerable: Iterable[Any]) -> List[int]:
    """[summary]

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
    """
    transitions = [ i+1 for i, (enumerable_at_i, enumerable_at_i_plus_one)
                    in enumerate(zip(enumerable[:-1], enumerable[1:]))
                    if enumerable_at_i != enumerable_at_i_plus_one ]
    return [0] + transitions + [len(enumerable)]

# TODO: example, test, what if enumerable_ = [], [False], [True], [True,True], [False,False]
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
    """
    return sorted(set(
        [
            holding 
            for etf, etf_holdings_dict in query_output.items()
            for holding in etf_holdings_dict.keys()
        ]
    ))

# TODO: example, test
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
        If kept as None, it gets converted to the output of `reorder_holdings_by_overlap`

    Returns
    -------
    Union[pd.DataFrame, Mapping[str, List[float]]]
        Dictionary mapping an ETF's ticker (string) to a list of floats. 
        Each position in the list of floats corresponds to a holding held by >= 1
        ETF in `query_output`. The float at position `i` indicates the weight of the
        `ith` holding in the corresponding ETF.
    """
    if all_holdings is None:
        all_holdings = [holding for holding, annotation in 
                        reorder_holdings_by_overlap(query_output)]

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

# TODO: example, tests
def weighted_jaccard_distance(v1: Iterable[float], v2: Iterable[float]) -> float:
    """Convenience function implementing the weighted Jaccard Distance metric.

    Parameters
    ----------
    v1 : Iterable[float]
        An iterable of floats.
    v2 : Iterable[float]
        An iterable of floats.

    Returns
    -------
    float
        The weighted Jaccard Distance between `v1` and `v2`

    References
    ----------
    - https://en.wikipedia.org/wiki/Jaccard_index#Weighted_Jaccard_similarity_and_distance
    """
    return sum([min(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)])/sum([max(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)])

# TODO: example
def get_similarity( query_output: Mapping[str, Mapping[str, Mapping]],
                    distance_measure: Union[str,Callable] = cosine) -> Mapping[Tuple[str,str], float]:
    """Wrapper around the functions for the supported distance measures 
    (Cosine Distance, Jaccard Distance, and weighted Jaccard Distance).

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
        (must be one of 'cosine','jaccard','weighted_jaccard'), or the function
        itself. 
        By default cosine

    Returns
    -------
    Mapping[Tuple[str,str], float]
        Dictionary mapping a tuple of strings (two ETF tickers) to their distance
        (according to the chosen metric).
    """
    if isinstance(distance_measure, str):
        distance_measure = distance_measure.lower()
        assert distance_measure in ('cosine','jaccard','weighted_jaccard'), \
            f"{distance_measure} is not among the supported distance measures ('cosine','jaccard', 'weighted_jaccard')"
        distance_measure_to_function = {
            'jaccard': jaccard,
            'cosine': cosine,
            'weighted_jaccard': weighted_jaccard_distance
        }
        distance_measure = distance_measure_to_function[distance_measure]
        
    assert distance_measure in (cosine, jaccard, weighted_jaccard_distance)
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

# TODO: test, example
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

# TODO: test, example
def reorder_holdings_by_overlap(query_output: Mapping[str, Mapping[str, Mapping]]) -> List[Tuple[str,str]]:
    """Convenience wrapper around `annotate_holdings` that sorts its result such that
    the holding tickers (strings) held by the most ETFs are first in the sequence."""
    return sorted(
        annotate_holdings(query_output).items(),
        key = lambda item: item[1],
        reverse = True
    )


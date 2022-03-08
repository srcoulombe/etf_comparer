# test_utils.py 

# standard library dependencies

# external dependencies
import pytest 

# local dependencies
from src.utils import (
    get_boundaries,
    get_contiguous_truthy_segments,
    get_etf_holding_weight_vectors,
    weighted_jaccard_distance,
    get_similarity,
    annotate_holdings,
    reorder_holdings_by_popularity
)

get_boundaries_tests = [
    ([], [0, 0]),
    ([0], [0, 1]),
    ([0, 1], [0, 1, 2]),
    ([-1, 0], [0, 1, 2]),
    ([0, 1, 2], [0, 1, 2, 3]),
    ([1, 1, 1], [0, 3]),
    ([11, 11, 11], [0, 3]),
    ([0, 0, 0], [0, 3]),
]
@pytest.mark.parametrize("case,answer", get_boundaries_tests)
def test_get_boundaries(case, answer):
    assert get_boundaries(case) == answer

get_contiguous_truthy_segments_tests = [
    ([], []),
    ([False], []),
    ([False, 0], []),
    ([0, 0], []),
    ([False, False], []),
    ([0], []),
    ([1], [(0,1)]),
    ([True], [(0,1)]),
    ([-1], [(0,1)]),
    ([0, 1], [(1,2)]),
    ([-1, 0], [(0,1)]),
    ([1,1,0], [(0,2)]),
    ([0,1,0,1,1,1], [(1,2),(3,6)]),
    ([1,1,1,1,1,1], [(0,6)]),
]
@pytest.mark.parametrize("case,answer", get_contiguous_truthy_segments_tests)
def test_get_contiguous_truthy_segments(case, answer):
    assert get_contiguous_truthy_segments(case) == answer

get_etf_holding_weight_vectors_tests = [
    (
        {'etf1': {'A': {'weight': 0.2}, 'B': {'weight': 0.3}}, 'etf2': {'C': {'weight': 1.0}}}, 
        {"etf1": [0.2, 0.3, 0.0], "etf2": [0.0, 0.0, 1.0]}
    ),
    (
        {'etfX': {'A': {'weight': 0.2}, 'B': {'weight': 0.3}}, 'etf2': {'C': {'weight': 1.1}}}, 
        {"etfX": [0.2, 0.3, 0.0], "etf2": [0.0, 0.0, 1.1]}
    ),
    (
        {'etf1': {'A': {'weight': 0.2}, }, 'etf2': {'C': {'weight': 0.9}}}, 
        {"etf1": [0.2, 0.0], "etf2": [0.0, 0.9]}
    ),
]
@pytest.mark.parametrize("case,answer", get_etf_holding_weight_vectors_tests)
def test_get_etf_holding_weight_vectors(case, answer):
    assert get_etf_holding_weight_vectors(case) == answer

weighted_jaccard_distance_tests = [
    (([1, 0, 0], [1, 0, 0]), 0),
    (([1, 0, 0], [0, 0, 0]), 1),
    (([0, 0, 0], [1, 0, 0]), 1),
    (([1, 0], [0, 1]), 1),
    (([2, 0, 0], [2, 0, 3]), 0.6),
    (([2, 0, 3], [2, 0, 0]), 0.6),
    (([2, 1, 0], [2, 1, 3]), 0.5),
    (([.2, .1, .0], [.2, .1, .3]), 0.5),
    (([0, 0], [0, 0]), 0),
    (([2], [2]), 0),
    (([2], [1]), 0.5),
]
@pytest.mark.parametrize("case,answer", weighted_jaccard_distance_tests)
def test_weighted_jaccard_distance(case, answer):
    assert weighted_jaccard_distance(*case) == answer

weighted_jaccard_distance_tests = [
    ([1, 0], [1, 0, 0]),
    ([1, 0], [1]),
    ([0, -1], [1, 1]),
    ([0, -1], [1, -1]),
    ([0, 1], [-1, 1]),
    ([], [1]),
    ([1,],[])
]
@pytest.mark.parametrize("case", weighted_jaccard_distance_tests)
def test_weighted_jaccard_distance(case):
    with pytest.raises(AssertionError):
        weighted_jaccard_distance(*case)

get_similarity_tests = [
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}},
        "jaccard",
        0.0
    ),
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}},
        "weighted_jaccard",
        0.0
    ),
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerA": {"weight": 0.1}, "tickerD": {"weight": 0.3}, "tickerE": {"weight": 0.3}, "tickerF": {"weight": 0.3}}},
        "jaccard",
        0.2
    ),
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerA": {"weight": 0.1}, "tickerD": {"weight": 0.3}, "tickerE": {"weight": 0.3}, "tickerF": {"weight": 0.3}}},
        "weighted_jaccard",
        0.05
    ),
]
@pytest.mark.parametrize("case,distance,answer", get_similarity_tests)
def test_get_similarity(case, distance, answer):
    result = get_similarity(case, distance_measure=distance)
    similarity = result[("etf1","etf2")]
    assert round(similarity, 2) == answer

annotate_holdings_tests = [
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}},
        {"tickerA": '10', "tickerB": '10', "tickerC": '01'}
    ),
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerA": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerD": {"weight": 0.05}}},
        {'tickerA': '101', 'tickerB': '101', 'tickerC': '010', 'tickerD': '001'}
    ),
    (
        {"etf1": {"tickerD": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerD": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerA": {"weight": 0.05}}, "etfX": {"tickerD": {"weight": 1.0}}},
        {'tickerD': '1011', 'tickerB': '1010', 'tickerC': '0100', 'tickerA': '0010'}
    )
]
@pytest.mark.parametrize("case,answer", annotate_holdings_tests)
def test_annotate_holdings(case, answer):
    assert annotate_holdings(case) == answer


reorder_holdings_by_popularity_tests = [
    (
        {"etf1": {"tickerA": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}},
        [("tickerA", '10'), ("tickerB", '10'), ("tickerC", '01')]
    ),
    (
        {"etf1": {"tickerD": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerD": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerA": {"weight": 0.05}}, "etfX": {"tickerD": {"weight": 1.0}}},
        [('tickerD', '1011'), ('tickerB', '1010'), ('tickerC', '0100'), ('tickerA', '0010')]
    ),
    (
        {"etf1": {"tickerD": {"weight": 0.5}, "tickerB": {"weight": 0.5}}, "etf2": {"tickerC": {"weight": 1.0}}, "etf3": {"tickerD": {"weight": 0.9}, "tickerB": {"weight": 0.05}, "tickerA": {"weight": 0.05}}, "etfX": {"tickerD": {"weight": 1.0}}},
        [('tickerD', '1011'), ('tickerB', '1010'), ('tickerC', '0100'), ('tickerA', '0010')]
    )
]
@pytest.mark.parametrize("case,answer", reorder_holdings_by_popularity_tests)
def test_reorder_holdings_by_popularity(case, answer):
    assert reorder_holdings_by_popularity(case) == answer

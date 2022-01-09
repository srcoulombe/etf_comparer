# utils.py

# standard library dependencies
from typing import Mapping, List, Tuple, Callable, Union, Iterable

# external dependencies
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.spatial.distance import cosine, jaccard

plt.style.use('classic')
plt.rcParams.update({
    "figure.facecolor": '#0e1117',  
    "savefig.facecolor": '#0e1117',  
    "figure.edgecolor": 'white',
    "axes.facecolor": '#0e1117',  
    "axes.edgecolor": "white",
    "savefig.transparent": True,
    "savefig.pad_inches": 0.0,
    "grid.linestyle": "--",
    "grid.alpha": 1.0,
    'figure.autolayout': True,
    'axes.xmargin': 0.0,
    'axes.ymargin': 0.0,
    'text.color': 'white',
    'patch.edgecolor': 'white',
    'axes.labelcolor': "white",
    'ytick.color': "white",
    'ytick.major.size': 1.0
})

def get_boundaries(enumerable) -> List[int]:
    transitions = [ i+1 for i, (enumerable_at_i, enumerable_at_i_plus_one)
                    in enumerate(zip(enumerable[:-1], enumerable[1:]))
                    if enumerable_at_i != enumerable_at_i_plus_one ]
    return [0] + transitions + [len(enumerable)]

def get_contiguous_truthy_segments(enumerable_) -> List[Tuple[int,int]]:
    enumerable = list(map(bool, enumerable_))
    contigs = []
    start = 0
    while start < len(enumerable):
        if enumerable[start] is False: 
            start += 1
        else:
            stop_exc = start
            while stop_exc < len(enumerable) and enumerable[stop_exc]:
                stop_exc += 1
            contigs.append((start, stop_exc))
            start = stop_exc
    return contigs

def get_all_holdings(query_output: Mapping[str, Mapping[str, Mapping]]) -> List[str]:
    return sorted(set(
        [
            holding 
            for etf, etf_holdings_dict in query_output.items()
            for holding in etf_holdings_dict.keys()
        ]
    ))

def get_etf_holding_weight_vectors( query_output: Mapping[str, Mapping[str, Mapping]],
                                    all_holdings: List[str] = None) -> Mapping[str, List[float]]:
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
    return etfs_as_vectors

def weighted_jaccard_distance(v1: Iterable[float], v2: Iterable[float]) -> float:
    return sum([min(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)])/sum([max(v1_i, v2_i) for (v1_i, v2_i) in zip(v1, v2)])

def get_similarity( query_output: Mapping[str, Mapping[str, Mapping]],
                    distance_measure: Union[str,Callable] = cosine) -> Mapping[Tuple[str,str], float]:
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

def annotate_holdings(query_output: Mapping[str, Mapping[str, Mapping]]) -> Mapping[str, str]:
    all_holdings_with_annotations: Mapping[str, tuple] = dict()
    for i, (etf, etf_holdings_dict) in enumerate(query_output.items()):
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

def reorder_holdings_by_overlap(query_output: Mapping[str, Mapping[str, Mapping]]) -> List[Tuple[str,str]]:
    return sorted(
        annotate_holdings(query_output).items(),
        key = lambda item: item[1],
        reverse = True
    )

def plot_holding_track( etf_name: str, 
                        holding_weight_vector: List[float], 
                        color = 'white',
                        ax = None):
    if ax is None:
        fig, ax = plt.subplots()
    ax.bar(
        list(range(len(holding_weight_vector))),
        holding_weight_vector,
        color = 'white'
    )
    ax.set_ylabel(f"% {etf_name}")

def plot_holdings_tracks(query_output: Mapping[str, Mapping[str, Mapping]]):
    etf_holding_weight_vectors = get_etf_holding_weight_vectors(query_output)
    fig, figax = plt.subplots(
        nrows = len(query_output)+1,
        figsize = (8, 2*len(query_output)),
        sharex = True
    )
    colours = list(mcolors.TABLEAU_COLORS.values())
    for i, (etf_name, etf_holding_weight_vector) in enumerate(etf_holding_weight_vectors.items()):
        plot_holding_track(
            etf_name,
            etf_holding_weight_vector,
            ax = figax[i]
        )

        bottom = i / len(etf_holding_weight_vectors)
        top = bottom + 1/len(etf_holding_weight_vectors)
        boundaries = get_contiguous_truthy_segments([i > 0 for i in etf_holding_weight_vector])
        
        for j, (start_inc, stop_exc) in enumerate(boundaries):
            figax[~0].axvspan(
                start_inc, 
                stop_exc, 
                ymin = bottom,
                ymax = top,
                zorder = -1,
                color = colours[i%len(colours)],
                alpha = 0.75,
                label = etf_name if j == 0 else None
            )
    figax[~0].set_ylabel("ETF Coverage")
    figax[~0].set_yticks([])
    figax[~0].set_xticks([])
    
    # Put a legend below current axis
    figax[~0].legend(
        loc = 'upper center', 
        bbox_to_anchor = (0.5, -0.25),
        fancybox = True, 
        shadow = True, 
        ncol = 10
    )
    for ax in figax:
        ax.set_alpha(0.0)
        ax.tick_params(axis='y', which='major', labelsize=8)
    figax[~0].set_xlabel("Holdings Ordered From Most -> Least Common")
    plt.tight_layout()
    plt.margins(0,0)
    return fig

def plot_similarity(query_output: Mapping[str, Mapping[str, Mapping]],
                    distance_measure: Union[str,Callable] = cosine):
    
    similarities = get_similarity(
        query_output,
        distance_measure = distance_measure
    )
    etf_names = sorted(
        set(etf_name for etf_pair in similarities.keys() for etf_name in etf_pair)
    )
    df = pd.DataFrame(
        np.ones((len(etf_names), len(etf_names))),
        index = etf_names,
        columns = etf_names
    )
    for (etf_1, etf_2), similarity in similarities.items():
        df.loc[etf_1, etf_2] = round(similarity, 3)
        df.loc[etf_2, etf_1] = round(similarity, 3)
    
    fig, ax = plt.subplots(figsize=(4,4))
    sns.heatmap(
        df, 
        annot = True, 
        cmap='Reds', 
        ax = ax,
        xticklabels = etf_names,
        yticklabels = etf_names,
        vmax = 1.,
        vmin = 0,
        fmt='.2%'
    )
    ax.tick_params(axis='both', which='both', labelsize=12, labelcolor='white')
    plt.tight_layout()
    return fig
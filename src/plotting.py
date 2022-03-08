# plotting.py 

# standard library dependencies
from typing import Mapping, List, Callable, Union

# external dependencies
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.spatial.distance import jaccard

# local dependencies
from .utils import get_etf_holding_weight_vectors, get_contiguous_truthy_segments, get_similarity

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

def plot_holding_track( etf_name: str, 
                        holding_weight_vector: List[float], 
                        color: str = 'white',
                        ax = None) -> None:
    """Convenience function used to plot the holdings and weights
    of an ETF as a bar plot.

    Parameters
    ----------
    etf_name : str
        Ticker of the ETF.
    holding_weight_vector : List[float]
        List of floats indicating the weight (>= 0.0) of each holding ticker in 
        the ETF's holdings. 
    color : str, optional
        Color to use when plotting the bar plot, by default 'white'.
    ax : [type], optional
        Matplotlib figure axis on which to plot the bar plot, by default None.
        Gets auto-generated if its default value of None is retained.
    """
    if ax is None:
        fig, ax = plt.subplots()
    ax.bar(
        list(range(len(holding_weight_vector))),
        holding_weight_vector,
        color = color
    )
    ax.set_ylabel(f"% {etf_name}")

def plot_holdings_tracks(query_output: Mapping[str, Mapping[str, Mapping]]) -> plt.Figure:
    """Convenience function used to plot the vertical span chart indicating
    which holdings are held by each ETF. 

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
    plt.Figure
        Matplotlib figure containing the vertical span chart
    """
    etf_holding_weight_vectors = get_etf_holding_weight_vectors(query_output)
    fig, figax = plt.subplots(
        nrows = len(query_output)+1,
        figsize = (10, min(10,2*len(query_output))),
        sharex = True
    )
    
    colours = list(mcolors.TABLEAU_COLORS.values())
    ylabels: List[str] = []
    for i, (etf_name, etf_holding_weight_vector) in enumerate(etf_holding_weight_vectors.items()):
        plot_holding_track(
            etf_name,
            etf_holding_weight_vector,
            ax = figax[i]
        )
        ylabels.append(etf_name)

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
    figax[~0].set_yticks(
        [ y-len(etf_holding_weight_vectors) for y in list(range(len(etf_holding_weight_vectors)+1)) ]
    )
    figax[~0].set_yticklabels([""]+ylabels)
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
    figax[~0].set_xlabel("Holdings")
    plt.tight_layout()
    plt.margins(0,0)
    return fig

def plot_similarity(query_output: Mapping[str, Mapping[str, Mapping]],
                    distance_measure: Union[str,Callable] = jaccard) -> plt.Figure:
    """Plots the annotated heatmap indicating the distance between each ETF.

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
        By default jaccard

    Returns
    -------
    plt.Figure
        Matplotlib figure containing the annotated heatmap indicating the distance 
        between each ETF
    """
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
    n_etfs_to_font_size = {
        2:10,
        3:10,
        4:10,
        5:10,
        6:9,
        7:8,
        8:7,
        9:6,
        10:5
    }
    sns.heatmap(
        df, 
        annot = True, 
        cmap='Reds', 
        ax = ax,
        xticklabels = etf_names,
        yticklabels = etf_names,
        vmax = 1.,
        vmin = 0,
        fmt='.1%',
        annot_kws={'fontsize':n_etfs_to_font_size[df.shape[0]]}
    )
    ax.tick_params(
        axis='both', 
        which='both', 
        labelsize=12, 
        labelcolor='white'
    )
    plt.tight_layout()
    return fig
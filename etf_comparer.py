
# standard library dependencies
import logging
import logging.config
from io import BytesIO
from typing import List

# external dependencies
import numpy as np
import pandas as pd
import streamlit as st 
from streamlit_tags import st_tags

# local dependencies
from src.backend import select_database
from src.utils import get_etf_holding_weight_vectors
from src.plotting import plot_holdings_tracks, plot_similarity

logging.config.fileConfig("loggingConfig.yml")

logging.info("Initiating Streamlit app")

st.set_page_config(layout='centered')

st.title('Comparing ETFs')

with st.expander("What's an ETF?"):
    st.markdown("""An **Exchange-Traded Fund (ETF)** is basically a variety-pack of other financial assets that you
can purchase exactly as you purchase/invest in other stocks.
    
ETFs are often themed (i.e., they can be a collection of stocks from the same industry, location, etc..), 
and some aim to track an index (e.g., the 'SPY' ETF aims to hold the same stocks listed in Standard & Poor's (S&P) 500 index).
    
ETFs have other advantages, such as allowing you to invest in assets that are otherwise difficult to access, 
but **the main takeaway is that an ETF is a collection of assets (formally referred to as 'holdings') that are bundled together**.""")


backend_option = st.selectbox(
    'Choose the database management system to use in the back end:',
    ('TinyDB','SQLite3')
)
st.write('Using:', backend_option)

dbc = select_database(backend_option)
logging.info(f"Connected to {backend_option} client instance")

@st.cache 
def clean_user_data(user_input: List[str]) -> List[str]:    
    return [
        ticker.strip().upper() 
        for ticker in user_input
        if ticker != "" 
    ]

def run(user_input: str) -> None:
    logging.info(f'Loading data for: {user_input}')
    etfs_data, unavailable_etfs = dbc.get_holdings_and_weights_for_etfs(
        clean_user_data(user_input)[:10]
    )
    if len(unavailable_etfs) > 0:
        warning = f"Failed to fetch data for the following ETFs: {', '.join(unavailable_etfs)}"
        logging.warning(warning)
        st.warning(warning)

    logging.info(f'Loaded data for: {user_input}')
    logging.info(f'Processing data for: {user_input}')
    
    st.pyplot(plot_holdings_tracks(etfs_data), dpi=1000)
    
    logging.info(f'Processed data for: {user_input}')

    logging.info(f'Calculating similarities between: {user_input}')

    logging.info(f'Processed data for: {user_input}')

    logging.info(f'Calculating similarities between: {user_input}')

    # plot the similarity matrices
    col1, col2 = st.columns([5,5])
    with col1:
        st.markdown(
            "<h4 style='text-align: center; color: white;'>Weighted Jaccard Similarity</h4>", 
            unsafe_allow_html = True
        )
        # plot the similarity matrix
        fig = plot_similarity(etfs_data, distance_measure='weighted_jaccard')
        buf = BytesIO()
        fig.savefig(buf, format="png")#, figsize=(4,4))
        st.image(buf)
    with col2:
        st.markdown(
            "<h4 style='text-align: center; color: white;'>Jaccard Similarity</h4>", 
            unsafe_allow_html = True
        )
        # plot the similarity matrix
        fig = plot_similarity(etfs_data, distance_measure='jaccard')
        buf = BytesIO()
        fig.savefig(buf, format="png")#, figsize=(4,4))
        st.image(buf)
    
    logging.info(f'Calculated similarities between: {user_input}')
    logging.info(f'Re-fetching data for: {user_input}')
    logging.info(f'Calculated similarities between: {user_input}')
    logging.info(f'Re-fetching data for: {user_input}')
    data = get_etf_holding_weight_vectors(
        etfs_data, 
        as_df = True
    )
    logging.info(f'Re-fetched data for: {user_input}')
    
    st.subheader("Data")
    with st.expander("Show Data"):
        st.markdown("""The following data indicates the weight given to each holding in the chosen ETFs.""")
        st.download_button(
            "Press to Download",
            data.to_csv().encode('utf-8'),
            "file.csv",
            "text/csv",
            key='download-csv'
        )
        st.dataframe(data)
    
st.subheader("Specify up to 10 ETFs to compare")

with st.form(key='my_form'):
    user_input = st_tags(
        label = " Press ENTER to add an ETF ticker.",
        value = ["SPY", "QQQ", "DIA"],
        maxtags = 10,
        suggestions = dbc.get_known_etfs()
    )
    submit_button = st.form_submit_button(label='Launch')

if submit_button:
    run(user_input)
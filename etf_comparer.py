
# standard library dependencies
from io import BytesIO
from typing import List

# external dependencies
import streamlit as st 
from streamlit_tags import st_tags

# local dependencies
from backend import DatabaseClient
from utils import plot_holdings_tracks, plot_similarity

dbc = DatabaseClient()

st.set_page_config(layout='centered')

st.title('Comparing ETFs')

@st.cache 
def clean_user_data(user_input: List[str]) -> List[str]:    
    return [
        ticker.strip().upper() 
        for ticker in user_input
        if ticker != "" 
    ]

def run(user_input: str) -> None:
    print('Loading data...')
    etfs_data = dbc.query(
        clean_user_data(user_input)
    )
    print('Loaded data.')
    print('Processing data...')
    st.pyplot(plot_holdings_tracks(etfs_data), dpi=1000)

    # plot the similarity matrices
    col1, col2 = st.columns([5,5])
    with col1:
        st.markdown(
            "<h4 style='text-align: center; color: white;'>Cosine Similarity</h4>", 
            unsafe_allow_html = True
        )
        # plot the similarity matrix
        fig = plot_similarity(etfs_data, distance_measure='cosine')
        buf = BytesIO()
        fig.savefig(buf, format="png", figsize=(4,4))
        st.image(buf)
    with col2:
        st.markdown(
            "<h4 style='text-align: center; color: white;'>Jaccard Similarity</h4>", 
            unsafe_allow_html = True
        )
        # plot the similarity matrix
        fig = plot_similarity(etfs_data, distance_measure='jaccard')
        buf = BytesIO()
        fig.savefig(buf, format="png", figsize=(4,4))
        st.image(buf)

    #st.pyplot(plot_similarity(etfs_data), figsize=(2,2), dpi=1000)
st.subheader("Specify up to 20 ETFs to compare")

with st.form(key='my_form'):
    user_input = st_tags(
        label = " Press ENTER to add an ETF ticker.",
        value = ["SPY", "QQQ", "DIA"],
        maxtags = 20,
        suggestions = dbc.get_known_etfs()
    )
    submit_button = st.form_submit_button(label='Launch')


if submit_button:
    run(user_input)
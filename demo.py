
# standard library dependencies
from typing import List

# external dependencies
import streamlit as st 
from streamlit_tags import st_tags

# local dependencies
from backend import DatabaseClient
from utils import plot_holdings_tracks

dbc = DatabaseClient()

st.title('Comparing ETFs')

@st.cache 
def clean_user_data(user_input_text: List[str]) -> List[str]:    
    return [
        ticker.strip().upper() 
        for ticker in user_input
        if ticker != "" 
    ]

def run(user_input_text: str) -> None:
    print('Loading data...')
    etfs_data = dbc.query(
        clean_user_data(user_input)
    )
    print('Loaded data.')
    print('Processing data...')
    st.pyplot(plot_holdings_tracks(etfs_data))
    
st.subheader("Specify which ETFs to compare")

user_input = st_tags(
    label = "Enter up to 20 ETFs' tickers",
    text = "Press ENTER to add more ETF tickers",
    value = ["SPY", "QQQ", "DIA"],
    maxtags = 20,
    suggestions = dbc.get_known_etfs()
)
if st.button("Launch"):
    run(user_input)

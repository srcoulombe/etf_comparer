
# standard library dependencies
from typing import List

# external dependencies
import streamlit as st 

# local dependencies
from backend import DatabaseClient
from utils import plot_holdings_tracks

dbc = DatabaseClient()

st.title('Comparing ETFs')

@st.cache 
def clean_user_data(user_input_text: str) -> List[str]:    
    # remove all whitespace
    user_input = "".join(user_input_text.split())
    return [
        ticker.upper() 
        for ticker in user_input.split(",") 
        if ticker != "" 
    ]

def run(user_input_text: str) -> None:
    data_load_state = st.text('Loading data...')
    etfs_data = dbc.query(
        clean_user_data(user_input)
    )
    data_load_state = st.text('Loaded data.')
    data_load_state = st.text('Processing data...')
    st.pyplot(
        plot_holdings_tracks(etfs_data)
    )
    
    
st.subheader("Specify which ETFs to compare")
user_input = st.text_area(
    "(Use commas to separate the ETFs' tickers)", 
    "SPY, QQQ, DIA"
)
if st.button("Launch"):
    run(user_input)

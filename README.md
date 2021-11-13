# ETF Analyzer
Small streamlit app used to visualize the similarity between ETFs' holdings.

# Architecture
## Backend
LRU cache'd `TinyDB` database
Data downloads are done using `requests`

## Frontend
`Streamlit`

## TODO
### Development
[x] - replace streamlit `text_area` with smaller input field
[ ] - add description of what an ETF is and the business case for this app
[x] - add tags to input field
    [x] - adjust DatabaseClient to provide previously-seen tags
[x] - develop similarity matrix functionality
[x] - develop similarity matrix plotting
[ ] - add tabs to streamlit app
    [ ] - requires replacing `matplotlib` with `bokeh`
[ ] - some minor refactoring
[ ] - replace `TinyDB` with another document database
[ ] - documentation
[ ] - ability to scrape additional sources

### Deployment
[ ] - put on heroku

### Extension
What would be **REALLY** useful is the ability to return the `k` ETFs that are the most different from a collection of ETFs.
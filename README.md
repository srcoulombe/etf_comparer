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
[ ] - replace streamlit `text_area` with smaller input field
[ ] - add description of what an ETF is and the business case for this app
[ ] - add tags to input field
    [ ] - adjust DatabaseClient to provide previously-seen tags
[ ] - develop similarity matrix functionality
[ ] - develop similarity matrix plotting
[ ] - add tabs to streamlit app
### Deployment
[ ] - replace TinyDB with another document database
[ ] - put on heroku
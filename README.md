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
    [ ] - sqlite3
    [ ] - postgres
    [ ] - postgres + timescaledb
[ ] - documentation
[ ] - ability to scrape additional sources
[ ] - ability to track ETFs over time
    [ ] - requires periodic scraping (like a cron-job)
    [ ] - requires adjusting the database query to specify the time
[ ] - push to github
    [ ] - pull on t450
    [ ] - branch for archive

### Deployment
[ ] - put on heroku

### Extension
What would be **REALLY** useful is the ability to return the `k` ETFs that are the most different from a collection of ETFs.

## Journal
OSS like [yahooquery](https://yahooquery.dpguthrie.com/) provide similar yet incomplete functinoality; they only return the ETFs' top 10 holdings.

Some other projects are interesting... https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjM2pP0jZv0AhUDHc0KHZjhBmMQFnoECAMQAQ&url=https%3A%2F%2Fmedium.com%2Fanalytics-vidhya%2Fdont-screen-etfs-pick-flowers-47aad109d1f9&usg=AOvVaw04Pu6mg9doCvmBtCXGGUpn

investpy

leoncvlt/etf4u on github

on the topic of cronjobs on aws: https://www.freecodecamp.org/news/how-to-create-auto-updating-data-visualizations-in-python-with-matplotlib-and-aws/

also found this: https://github.com/wilsonfreitas/awesome-quant#python


Other tools do exist but either focus on 1-vs-1 comparisons (https://etfdb.com/tool/etf-comparison/IVV-SPY/#holdings, https://www.etfrc.com/funds/overlap.php, https://www.etf.com/etfanalytics/etf-comparison/, ). Other tools allow for more than a 1-vs-1 comparison (https://www.vanguardcanada.ca/individual/insights/fundcompare.htm#/target=fct&selectedFund0=F000011X31&selectedFund1=F00000NF6Q&selectedFund2=F00000NF6R&selectedFund3=F0000101Q1, https://research.tdameritrade.com/grid/public/etfs/compare/compareResults.asp?tab=Snapshot&data=B64ENCeyJzeW1ib2xTaW5nbGUiOiJTeW1ib2wuLi4iLCJzeW1ib2xNdWx0aXBsZSI6IlNQWSwgUVFRLCBJVlYiLCJjb21wYXJlZFRvIjoiRVRGIiwiY3VycmVudFBhZ2UiOiJjb21wYXJlLmFzcCIsInRhYiI6IlNuYXBzaG90IiwiZm9ybVdhc1VzZWQiOnRydWUsImNvbXBhcmVUeXBlIjoic3BlY2lmaWMifQ==) but dont do this kind of overlap or are behind login walls (https://www.fundvisualizer.com/how-to-compare/etfs/comparison-charts.html)

Began using etf4u - something of a starting point, but needs more qork to be useful for this project

## IDEA
a query is a tuple of (etf_ticker: str, datetime)
    runs db query
    if comes up empty
        run etf4u_mod on the etf_ticker
        try to add to db

db schema
    etf_holdings_table
    brainstormed schema:
        row_id: int prim key
        date: date
        etf_ticker: str
        holdings: List[str]
        holding_weight: List[float]
    
    schema to statisfy 1NF:
        row_id: int prim key
        date: date
        etf_ticker: str
        holding: str
        holding_weight: float

    schema to satisfy 1,2,3NF:
        etf_holdings_table
            row_id: int prim key
            date: date
            etf_ticker: str
            holding_id: int
            holding_weight: int

        holdings_table:
            holding_id: int
            holding: str

    indices:
        etf_holdings_table:
            row_id
            date
            etf_ticker
        
        holdings_table:
            holding_id

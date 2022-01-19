# ETF Analyzer
Small streamlit app used to visualize the similarity between ETFs' holdings.

The app is now live on [Google Cloud Platform](https://test-streamlit-app.ue.r.appspot.com/)! Check it out there!

# Quickstart
0. Clone this repo using: `git clone https://github.com/srcoulombe/etf_comparer.git`
1. Navigate into the repository's directory and create a virtual environment by using: `python3 -m venv etfcomparer_venv`
2. Activate the virtual environment by using: `source etfcomparer_venv/bin/activate` if you're using MacOS/Linux or `.\etfcomparer_venv\Scripts\activate` if you're a Windows user.
3. Install the dependencies into your virtual environment by using: `python3 -m pip install -r requirements.txt`
4. Run the app by using: `streamlit run etf_comparer.py`


# Architecture
## Frontend
The frontend is built around `streamlit`, `streamlit-tags`, `matplotlib`, and `seaborn`.

## Backend
The user can choose which database management system to use at runtime. The options are: `sqlite3` and `tinydb`. 
I've chosen to use this project to compare and contrast the SQL and NoSQL approaches. 
Future work will include the development and deployment of more production-ready databases (specifically `postgresql` and `mongodb`).
Data scraping is done using the `requests` library. The constants (urls, integer IDs, etc...) for the Invesco, iShares, and ARK scrapers were obtained from [`etf4u`](https://github.com/leoncvlt/etf4u), but those scrapers were refactored. 

# TODO
## Development
- [ ] ability to scrape additional sources
- [ ] ability to track ETFs over time
    - [ ] requires periodic scraping (like a cron job). GCP and Heroku provide cron job services
- [ ] add functionality for db rollbacks
- [x] add functionality for `postgresql`
- [ ] add functionality for `mongodb`
- [ ] re-deploy with the new database management systems
- [x] add logging functionality
- [x] add a "show raw data" option
- [x] add a "download raw data" option
- [ ] add diagrams explaining database layouts
- [ ] add tab explaining distance measures

## Extension
What would be **REALLY** useful is the ability to return the `k` ETFs that are the most different from a collection of ETFs.

# Journal
OSS like [yahooquery](https://yahooquery.dpguthrie.com/) provide similar though restricted functionality: they only return the ETFs' top 10 holdings.


I also found some other pertinent projects. [Some are... interesting](https://medium.com/analytics-vidhya/dont-screen-etfs-pick-flowers-47aad109d1f9) to say the least.


Some projects like [`investpy`](https://investpy.readthedocs.io/) and the [GitHub repo `awesome-quant`](https://github.com/wilsonfreitas/awesome-quant#python) are more pertinent. I'll be looking into `investpy`'s ETF scraping capabilities to see what I could learn and use.

Other tools do exist but focus on 1-vs-1 comparisons ([etfdb](https://etfdb.com/tool/etf-comparison/IVV-SPY/#holdings), [etfrc](https://www.etfrc.com/funds/overlap.php), [etfanalytics](https://www.etf.com/etfanalytics/etf-comparison/)). Other tools scale beyond head-to-head comparisons ([Vanguard's FundCompare](https://www.vanguardcanada.ca/individual/insights/fundcompare.htm), [TD Ameritrade](https://research.tdameritrade.com/grid/public/etfs/compare/compareResults.asp?)), but don't do this type of overlap analysis. Some tools like [fundvisualizer](https://www.fundvisualizer.com/how-to-compare/etfs/comparison-charts.html) might be relevant, but they are behind paywalls so I wasn't able to look into them very much.

Notes on the topic of cron jobs:
- [on aws](https://www.freecodecamp.org/news/how-to-create-auto-updating-data-visualizations-in-python-with-matplotlib-and-aws/)
- with a fastapi endpoint [1](https://github.com/streamlit/streamlit/issues/439#issuecomment-1007204213), [2](https://davidefiocco.github.io/streamlit-fastapi-ml-serving/), [3](https://cloud.google.com/appengine/docs/flexible/python/scheduling-jobs-with-cron-yaml)




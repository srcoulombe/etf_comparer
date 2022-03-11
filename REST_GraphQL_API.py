# 

# standard library dependencies
import typing

# external dependencies
import uvicorn
import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI

# local dependencies
from src.dbms.postgrestest import PostgresDatabaseClient

app = FastAPI(
    title="ETF Comparer REST API",
    description="API to return ETF holdings data",
    version="0.1"
)

@app.get("/")
def hello_world():
    return {"message": "Hello world!"}

@app.get("/etf/{etf_ticker}")
def get_etf_holdings(etf_ticker: str):
    postgres_db_client = PostgresDatabaseClient(
        "aws_credentials.json"
    )
    etf_ticker = etf_ticker.lower()
    etf_holdings = postgres_db_client.get_holdings_and_weights_for_etf(
        etf_ticker
    )
    holdings = [
        {
            "date": etf_holding[0],
            "holding_ticker": etf_holding[2],
            "weight": etf_holding[3]
        }
        for etf_holding in etf_holdings
    ]
    return {"etf_ticker": etf_ticker, "holdings": holdings}

@app.get("/etfs")
def get_etf_holdings():
    postgres_db_client = PostgresDatabaseClient(
        "aws_credentials.json"
    )
    etfs = postgres_db_client.get_known_etfs()
    return {"known_etfs": etfs}

@strawberry.type
class HoldingData:
    holding_ticker: str
    date: str
    weight: float

@strawberry.type
class ETF:
    ticker: str
    holdings: typing.List[HoldingData]

@strawberry.type
class Query:
    @strawberry.field
    def get_known_etfs(self) -> typing.List[str]:
        postgres_db_client = PostgresDatabaseClient(
            "aws_credentials.json"
        )
        etfs = postgres_db_client.get_known_etfs()
        return etfs

    @strawberry.field
    def get_etf_holdings(self, etf_ticker: str) -> ETF:
        postgres_db_client = PostgresDatabaseClient(
            "aws_credentials.json"
        )
        etf_ticker = etf_ticker.lower()
        
        etf_holdings = postgres_db_client.get_holdings_and_weights_for_etf(
            etf_ticker
        )
        holdings = [
            HoldingData(
                holding_ticker = str(etf_holding[2]),
                date = str(etf_holding[0]),
                weight = float(etf_holding[3])
            )
            for etf_holding in etf_holdings
        ]
        return ETF(ticker=etf_ticker, holdings=holdings)

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8887)

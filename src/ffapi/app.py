from typing import Union

from fastapi import FastAPI, HTTPException
import pandas as pd

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/sample")
def sample_data():
    df = pd.read_parquet("/Users/seon-u/code/ffapi/data")
    sample_df = df.sample(n=5)
    r = sample_df.to_dict(orient='records')

    return r

@app.get("/movie/{movie_cd}")
def movie_meta(movie_cd: int):
    df = pd.read_parquet("/Users/seon-u/code/ffapi/data")
    meta_df = df[['movieCd'] == movie_cd]

    if meta_df.empty:
        raise HTTPException(status_code=404, detail="영화를 찾을 수 없습니다.")

    r = meta_df.iloc[0].to_dict()

    return r

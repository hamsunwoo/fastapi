from typing import Union

from fastapi import FastAPI, HTTPException
import pandas as pd
import requests
import os

app = FastAPI()

df = pd.read_parquet("/Users/seon-u/code/ffapi/data")

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key

def gen_url(movie_cd):
    base_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    key = get_key()
    url = f"{base_url}?key={key}&movieCd={movie_cd}"
    return url

def req(movie_cd):
    url = gen_url(movie_cd)
    r = requests.get(url)
    data = r.json()
    return data

def req2list(movie_cd):
    data = req(movie_cd)
    l = data["movieInfoResult"]["movieInfo"]
    return l

@app.get("/sample")
def sample_data():
    sample_df = df.sample(n=5)
    r = sample_df.to_dict(orient='records')

    return r

@app.get("/movie/{movie_cd}")
def movie_meta(movie_cd: str):
    #성능에 영향을 얼마나 줄까?
    #df = pd.read_parquet("/Users/seon-u/code/ffapi/data")
    meta_df = df[df['movieCd'] == movie_cd]

    if meta_df.empty:
        raise HTTPException(status_code=404, detail="영화를 찾을 수 없습니다.")
   
    r = meta_df.iloc[0].to_dict()

    if r['repNationCd'] is None:
        d = req2list(movie_cd)
        nn = d['nations'][0]['nationNm']

        if nn != "한국":
            r['repNationCd'] = 'F'
        else:
            r['repNationCd'] = 'K'
    

    return r

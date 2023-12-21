import pandas as pd
import os

path = os.path.dirname(__file__)

movie_tag = pd.read_csv(path+'/../data/Movie_tag.csv')
douban2fb = pd.read_csv(path+'/../data/douban2fb.csv')
movie = pd.merge(movie_tag,douban2fb,how='inner',on='id')
# print(movie.head(5))




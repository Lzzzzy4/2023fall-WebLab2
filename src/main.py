import pandas as pd
import os

path = os.path.dirname(__file__)

movie_tag = pd.read_csv(path+'/../data/Movie_tag.csv')
movie_tag = movie_tag.set_index('id')['tag']
# print(movie_tag[1291543])

douban2fb = pd.read_csv(path+'/../data/douban2fb.csv')
douban2fb = douban2fb.set_index('id')['entity']
# print(douban2fb.head())


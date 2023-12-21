import pandas as pd
import os
import gzip

path = os.path.dirname(__file__)

movie_tag = pd.read_csv(path+'/../data/Movie_tag.csv')
douban2fb = pd.read_csv(path+'/../data/douban2fb.csv')
df = pd.merge(movie_tag,douban2fb,how='inner',on='id')
df.set_index('entity',inplace=True)
df['tag'] = df['tag'].apply(lambda x: x.split(','))
# print(df.head(5))

with gzip.open(path+'/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    for line in f:
        i = i + 1
        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if(i < 10):
            print(triplet)



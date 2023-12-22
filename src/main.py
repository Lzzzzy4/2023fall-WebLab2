import pandas as pd
import os
import gzip
import json

path = os.path.dirname(__file__)

movie_tag = pd.read_csv(path+'/../data/Movie_tag.csv')
douban2fb = pd.read_csv(path+'/../data/douban2fb.csv')
df = pd.merge(movie_tag,douban2fb,how='inner',on='id')
df.set_index('entity',inplace=True)
df['tag'] = df['tag'].apply(lambda x: x.split(','))
print(df.head(5))

df_json = df.to_json(orient='index', path_or_buf=path+'/../data/movie_entity.json', force_ascii=False, indent=4)

with gzip.open(path+'/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    for line in f:
        # break
        i = i + 1
        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if(i < 10):
            print(triplet)
        
        patten = "<http://rdf.freebase.com/ns/"

        if(patten not in triplet[0]):
            continue
        item1 = triplet[0].split(patten)[1][:-1]

        if(patten not in triplet[2]):
            continue
        item2 = triplet[2].split(patten)[1][:-1]






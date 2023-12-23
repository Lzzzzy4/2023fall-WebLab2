import pandas as pd
import os
import gzip
import copy
import json
import time
import datetime

path = os.path.dirname(__file__)

movie_tag = pd.read_csv(path+'/../data/Movie_tag.csv')
douban2fb = pd.read_csv(path+'/../data/douban2fb.csv')
df = pd.merge(movie_tag,douban2fb,how='inner',on='id')
df.set_index('entity',inplace=True)
df['tag'] = df['tag'].apply(lambda x: x.split(','))
# print(df.head(5))

df.to_json(orient='index', path_or_buf=path+'/../data/movie_entity.json', force_ascii=False, indent=4)

movie_list = set(df.index.tolist())
graph = copy.deepcopy( df.drop(['id','tag'],axis=1) )
graph.insert(loc = 0,column = 'is_movie', value = 1)
graph.insert(loc = 1,column = 'count', value = 0)
graph.insert(loc = 2,column = 'content', value = None)
graph['content'] = graph['content'].apply(lambda x: [])
# print(graph.head(5))

st = time.time()
with gzip.open(path+'/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    cnt = 0
    for line in f:
        # break
        i = i + 1
        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if(i % 1000000 == 0):
            # print(triplet)
            print(i/395577070 * 100 , '%')
            last_time =(1-i/395577070) * (time.time() - st)/i * 395577070 
            print("剩余时间：",datetime.timedelta(seconds=last_time))
            print(cnt)
            cnt = 0
        # if(i == 10000000):
        #     break
        
        patten = "<http://rdf.freebase.com/ns/"
        if(patten != triplet[0][:len(patten)] or patten != triplet[2][:len(patten)]):
            continue
        item1 = triplet[0][len(patten):-1]
        item2 = triplet[2][len(patten):-1]
        relation = triplet[1]

        # 一跳子图
        if(item1 in movie_list):
            cnt = cnt + 1
            graph.loc[item1,'content'].append((relation,item2))
            graph.loc[item1,'count'] = graph.loc[item1,'count'] + 1
        if(item2 in movie_list):
            cnt = cnt + 1
            if (item1 not in graph.index):
                graph.loc[item1] = [0,0,[]]
            graph.loc[item1,'content'].append((relation,item2))
            graph.loc[item1,'count'] = graph.loc[item1,'count'] + 1

    print(i) #395577070
    # 过滤
    graph.insert(loc = 3,column = 'delete', value = 0)
    for index, row in graph.iterrows():
        if(row['is_movie'] == 0 and row['count'] < 20):
            graph.loc[index,'delete'] = 1

    graph = graph[graph['delete'] == 0]
    graph.drop(['delete'],axis=1,inplace=True)

    graph.to_json(orient='index', path_or_buf=path+'/../data/movie_graph.json', force_ascii=False, indent=4)

    end = time.time()
    print("用时：",datetime.timedelta(seconds=end-st))
        
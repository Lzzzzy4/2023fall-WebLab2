import pandas as pd
import os
import gzip
import copy
import json

path = os.path.dirname(__file__)

graph = pd.read_json(path+'/../data/movie_graph.json',encoding="utf-8", orient='index')
movie_list = graph.index.tolist()
graph.insert(loc = 3,column = 'is_first_step', value = 1)
print(graph.head(5))

with gzip.open(path+'/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    for line in f:
        # break
        i = i + 1
        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if(i % 10000 == 1):
            print(triplet)
        
        if(i == 100000):
            break
        
        patten = "<http://rdf.freebase.com/ns/"

        if(patten not in triplet[0]):
            continue
        item1 = triplet[0].split(patten)[1][:-1]

        if(patten not in triplet[2]):
            continue
        item2 = triplet[2].split(patten)[1][:-1]

        relation = triplet[1]

        # 一跳子图
        if(item1 in movie_list):
            if relation not in graph.loc[item1,'content'].keys():
                graph.loc[item1,'content'][relation] = []
            graph.loc[item1,'content'][relation].append(item2)
            graph.loc[item1,'count'] = graph.loc[item1,'count'] + 1
        if(item2 in movie_list):
            if (item1 not in graph.index):
                graph.loc[item1] = [0,0,{},0]
            if relation not in graph.loc[item1,'content'].keys():
                graph.loc[item1,'content'][relation] = []
            graph.loc[item1,'content'][relation].append(item2)
            graph.loc[item1,'count'] = graph.loc[item1,'count'] + 1

    print(i)
    # 过滤
    graph.insert(loc = 4,column = 'delete', value = 0)
    for index, row in graph.iterrows():
        if(row['is_first_step'] == 0 and row['count'] < 15):
            graph.loc[index,'delete'] = 1
            continue
        # for key in row['content'].keys():
        #     if(len(row['content'][key]) < 3):
        #         row['content'].pop(key)

    graph = graph[graph['delete'] == 0]
    graph.drop(['delete'],axis=1,inplace=True)

    graph.to_json(orient='index', path_or_buf=path+'/../data/movie_graph_2step.json', force_ascii=False, indent=4)
        
            



        






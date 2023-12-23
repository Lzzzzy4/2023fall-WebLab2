import pandas as pd
import os
import gzip
import copy
import json
import datetime
import time
import multiprocessing as mp

path = os.path.dirname(__file__)

graph = pd.read_json(path + '/../data/movie_graph.json',
                     encoding="utf-8",
                     orient='index')
step1_list = graph[graph['is_movie'] == 0].index.tolist()
step1_list = set(step1_list)
graph.insert(loc=3, column='is_first_step', value=1)

#graph to dict
# print(graph.head(5))

# 统计graph中所有关系的数量，过滤
relation_count = {}
for index, row in graph.iterrows():
    for (relation,item2) in row['content']:
        if relation not in relation_count:
            relation_count[relation] = 0
        relation_count[relation] = relation_count[relation] + 1

delete_list = []
for key in relation_count.keys():
    if (relation_count[key] < 500):
        delete_list.append(key)
for i in delete_list:
    relation_count.pop(i)

relation_list = list(relation_count.keys())
relation_list = set(relation_list)
print(len(step1_list), len(relation_list), len(graph))

st = time.time()

graph = graph.to_dict(orient='index')
with gzip.open(path + '/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    cnt = 0
    for line in f:
        i = i + 1

        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if (i % 100000 == 0):

            print(i / 395577070 * 100, '%')
            last_time = (1 - i / 395577070) * (time.time() -
                                               st) / i * 395577070
            print("剩余时间：", datetime.timedelta(seconds=last_time))
            print(cnt)
            print(len(graph))
            cnt = 0

        patten = "<http://rdf.freebase.com/ns/"

        relation = triplet[1]
        if (relation not in relation_list):
            continue

        if (patten not in triplet[0] or patten not in triplet[2]):
            continue
        item1 = triplet[0][len(patten):-1]
        item2 = triplet[2][len(patten):-1]

        if (item1 in step1_list):
            cnt = cnt + 1
            if relation not in graph[item1]['content']:
                graph[item1]['content'][relation] = []
            graph[item1]['content'][relation].append(item2)
            graph[item1]['count'] += 1
        if (item2 in step1_list):
            cnt = cnt + 1
            if (item1 not in graph):
                # graph.loc[item1] = [0, 0, {}, 0]
                graph[item1] = {
                    'is_movie': 0,
                    'count': 0,
                    'content': {},
                    'is_first_step': 0
                }
            if relation not in graph[item1]['content']:
                graph[item1]['content'][relation] = []
            graph[item1]['content'][relation].append(item2)
            graph[item1]['count'] += 1

    print(i)
    # 过滤
    graph.insert(loc=4, column='delete', value=0)
    for index, row in graph.iterrows():
        if (row['is_first_step'] == 0 and row['count'] < 30):
            graph.loc[index, 'delete'] = 1
            continue
        # for key in row['content'].keys():
        #     if(len(row['content'][key]) < 3):
        #         row['content'].pop(key)

    graph = graph[graph['delete'] == 0]
    graph.drop(['delete'], axis=1, inplace=True)

    graph.to_json(orient='index',
                  path_or_buf='E:/1.json',
                  force_ascii=False,
                  indent=4)

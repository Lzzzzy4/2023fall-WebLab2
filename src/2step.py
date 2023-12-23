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
step1_list = set(graph[graph['is_movie'] == 0].index.tolist())
graph.insert(loc=3, column='is_first_step', value=1)

#graph to dict
# print(graph.head(5))

# 统计graph中所有关系的数量，过滤
relation_count = {}
for index, row in graph.iterrows():
    for relation in row['content']:
        if relation not in relation_count:
            relation_count[relation] = 0
        relation_count[relation] = relation_count[relation] + len(row['content'][relation])
delete_list = []
for key in relation_count.keys():
    if (relation_count[key] < 100):
        delete_list.append(key)
for i in delete_list:
    relation_count.pop(i)
relation_list = set(relation_count.keys())

print("step1_list:", len(step1_list))
print("relation_list:", len(relation_list))
print("graph:", len(graph))

st = time.time()

graph = graph.to_dict(orient='index')
with gzip.open(path + '/../data/freebase_douban.gz', 'rb') as f:
    i = 0
    cnt = 0
    for line in f:
        i = i + 1

        # if(i < 350000000):
        #     continue

        line = line.strip()
        triplet = line.decode().split('\t')[:3]
        if (i % 1000000 == 0):

            print(i / 395577070 * 100, '%')
            last_time = (1 - i / 395577070) * (time.time() -
                                               st) / i * 395577070
            print("剩余时间：", datetime.timedelta(seconds=last_time))
            print("related items:", cnt)
            cnt = 0

        # if(i == 400000000):
        #     break

        patten = "<http://rdf.freebase.com/ns/"
        relation = triplet[1]
        # if (relation not in relation_list): 这里先不过滤
        #     continue
        if (patten != triplet[0][:len(patten)] or patten != triplet[2][:len(patten)]):
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
    delete_list = []
    for index in graph:
        if (graph[index]['is_first_step'] == 0 and graph[index]['count'] < 30):
            delete_list.append(index)

        # for key in row['content'].keys():
        #     if(len(row['content'][key]) < 3):
        #         row['content'].pop(key)

    for i in delete_list:
        graph.pop(i)
    
    # json.dump(graph, open(path + '/../data/movie_graph_2step_origin.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=4)
    json.dump(graph, open(path + '/../data/movie_graph_2step.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=4)
    # json.dump(graph, open('E:/2step_8.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=4)
    end = time.time()   
    print("用时：", datetime.timedelta(seconds=end - st))

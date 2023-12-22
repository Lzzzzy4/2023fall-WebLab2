import json
import os
import pandas as pd

path = os.path.dirname(__file__)

graph = json.load(open(path + '/movie_graph.json', 'r'))

douban2fb = pd.read_csv(path + '/douban2fb.csv')

movie_id_map = pd.read_csv(path + '/movie_id_map.csv')

output = open(path + '/kg_final.txt', 'w')

# trans movie_id_map to dict
movie_id_map_dict = {}
movie_id_dict = {}
relation_dict = {}
for i in range(len(movie_id_map)):
    movie_id_map_dict[movie_id_map.iloc[i]
                      ['id']] = movie_id_map.iloc[i]['index']

for i in range(len(douban2fb)):
    movie_id_dict[douban2fb.iloc[i]['entity']] = movie_id_map_dict[
        douban2fb.iloc[i]['id']]

movie_id_cnt = len(movie_id_dict)
relation_cnt = 0

for key in graph.keys():
    if key not in movie_id_dict.keys():
        movie_id_cnt += 1
        movie_id_dict[key] = movie_id_cnt
    u = movie_id_dict[key]
    for relation in graph[key]['content'].keys():
        if relation not in relation_dict.keys():
            relation_cnt += 1
            relation_dict[relation] = relation_cnt
        r = relation_dict[relation]
        for v in graph[key]['content'][relation]:
            if v not in movie_id_dict.keys():
                movie_id_cnt += 1
                movie_id_dict[v] = movie_id_cnt
            v = movie_id_dict[v]
            #output
            output.write(str(u) + '\t' + str(r) + '\t' + str(v) + '\n')

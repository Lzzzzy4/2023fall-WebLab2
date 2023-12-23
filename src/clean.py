import pandas as pd
import os
import json
import copy
path = os.path.dirname(__file__)
# 读取data/movie_graph_2step.json为dict
graph = pd.read_json(path + '/../data/movie_graph_2step.json', typ='series').to_dict()
print("loaded")

# 统计关系的数量
relation_cnt = {}
for item in graph:
    for key in graph[item]['content']:
        if(key not in relation_cnt):
            relation_cnt[key] = 0
        relation_cnt[key] = relation_cnt[key] + 1
print("relation_cnt:", len(relation_cnt))

# 删除关系数量小于50的关系
delete_list = []
for relation in relation_cnt:
    if(relation_cnt[relation] < 50):
        delete_list.append(relation)
print("delete_list:", len(delete_list))

for item in graph:
    list1 = copy.deepcopy(graph[item]['content'])
    for relation in list1:
        if(relation in delete_list):
            del graph[item]['content'][relation]
print("cleaned")

# 输出
json.dump(graph, open(path + '/../data/movie_graph_2step_cleaned.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=4)


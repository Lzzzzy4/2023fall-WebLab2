import pandas as pd
import os
import json
import copy
path = os.path.dirname(__file__)
# graph = pd.read_json(path + '/../data/movie_graph.json',
#                      encoding="utf-8",
#                      orient='index')
# # 统计节点个数，平均出现次数
# num_movie = 0
# num_first_step = 0
# sum_movie = 0
# sum_first_step = 0
# for index, row in graph.iterrows():
#     if (row['is_movie'] == 1):
#         sum_movie = sum_movie + row['count']
#         num_movie = num_movie + 1
#     else:
#         sum_first_step = sum_first_step + row['count']
#         num_first_step = num_first_step + 1

# print("num_movie:", num_movie)
# print("num_first_step:", num_first_step)
# print("num:", num_movie + num_first_step)
# print("sum_movie:", sum_movie)
# print("sum_first_step:", sum_first_step)
# print("sum:", sum_movie + sum_first_step)
# print("average_movie:", sum_movie / num_movie)
# print("average_first_step:", sum_first_step / num_first_step)
# print("average:", (sum_movie + sum_first_step) / (num_movie + num_first_step))

# # 统计关系的数量
# relation_cnt = {}
# relation_sum = 0
# for index, row in graph.iterrows():
#     for key in row['content']:
#         if (key not in relation_cnt):
#             relation_cnt[key] = 0
#         relation_cnt[key] = relation_cnt[key] + len(row['content'][key])
#         relation_sum = relation_sum + len(row['content'][key])
# print("relation_cnt:", len(relation_cnt))
# print("relation_sum:", relation_sum)
# print("average_relation:", relation_sum / len(relation_cnt))

# 统计二跳子图
graph = pd.read_json(path + '/../data/movie_graph_2step_cleaned.json',
                     encoding="utf-8",
                     orient='index')

num_movie = 0
num_first_step = 0
num_second_step = 0
sum_movie = 0
sum_first_step = 0
sum_second_step = 0
for index, row in graph.iterrows():
    if (row['is_movie'] == 1):
        sum_movie = sum_movie + row['count']
        num_movie = num_movie + 1
    elif (row['is_first_step'] == 1):
        sum_first_step = sum_first_step + row['count']
        num_first_step = num_first_step + 1
    else:
        sum_second_step = sum_second_step + row['count']
        num_second_step = num_second_step + 1

print("num_movie:", num_movie)
print("num_first_step:", num_first_step)
print("num_second_step:", num_second_step)
print("num:", num_movie + num_first_step + num_second_step)
print("sum_movie:", sum_movie)
print("sum_first_step:", sum_first_step)
print("sum_second_step:", sum_second_step)
print("sum:", sum_movie + sum_first_step + sum_second_step)
print("average_movie:", sum_movie / num_movie)
print("average_first_step:", sum_first_step / num_first_step)
print("average_second_step:", sum_second_step / num_second_step)
print("average:", (sum_movie + sum_first_step + sum_second_step) /
      (num_movie + num_first_step + num_second_step))

relation_cnt = {}
relation_sum = 0
for index, row in graph.iterrows():
    for key in row['content']:
        if (key not in relation_cnt):
            relation_cnt[key] = 0
        relation_cnt[key] = relation_cnt[key] + len(row['content'][key])
        relation_sum = relation_sum + len(row['content'][key])

print("relation_cnt:", len(relation_cnt))
print("relation_sum:", relation_sum)
print("average_relation:", relation_sum / len(relation_cnt))
    

# 假设有用户评价矩阵R
R = [[5, 3, 0, 1],
     [4, 0, 0, 1],
     [1, 1, 0, 5],
     [1, 0, 0, 4],
     [0, 1, 5, 4]]
# 假设有用户的相似度矩阵sim
sim = [[1, 0.2, 0.6, 0.8, 0.3],
       [0.2, 1, 0.3, 0.6, 0.2],
       [0.6, 0.3, 1, 0.3, 0.4],
       [0.8, 0.6, 0.3, 1, 0.4],
       [0.3, 0.2, 0.4, 0.4, 1]]
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# 将评分矩阵r转换为RDD
R_RDD = sc.parallelize(R)

# 将相似度矩阵sim转换为RDD
sim_RDD = sc.parallelize(sim)

# 将用户相似度矩阵sim转换为元组
sim_tuple = sim_RDD.map(lambda x: [(i,x[i]) for i in range(len(x))])

# 将用户评分矩阵R转换为元组
R_tuple = R_RDD.map(lambda x: [(i,x[i]) for i in range(len(x))])

# 将用户评分矩阵R和用户相似度矩阵sim联合
joined = sim_tuple.zip(R_tuple).map(lambda x: (x[0][0], x[1][0], x[0][1], x[1][1]))

# 计算推荐分数
score = joined.map(lambda x: (x[0], x[1], x[2] * x[3]))

# 计算每个用户的推荐分数
result = score.map(lambda x: (x[0], x[1], x[2])).reduceByKey(lambda x, y: x + y)

# 计算最终推荐结果
result.sortBy(lambda x: x[2], ascending=False).collect()
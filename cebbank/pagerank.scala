su -l hadoop

wget http://labfile.oss.aliyuncs.com/courses/534/data.tar.gz

tar -zxvf data.tar.gz

# 计算每一个用户的网页排名

// 读取各个边来组成图
val graph = GraphLoader.edgeListFile(sc, "/home/hadoop/followers.txt")

// 运行PageRank
val ranks = graph.pageRank(0.0001).vertices

// 连结用户名与评级
val users = sc.textFile("/home/hadoop/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}

// 输出结果
println(ranksByUsername.collect().mkString("\n"))

#计算连通分量

// 加载样本数据集中的图
val graph = GraphLoader.edgeListFile(sc, "/home/hadoop/followers.txt")

// 寻找连通分量
val cc = graph.connectedComponents().vertices

// 连结用户名和连通分量
val users = sc.textFile("/home/hadoop/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}
val ccByUsername = users.join(cc).map {
  case (id, (username, cc)) => (username, cc)
}

// 输出结果
println(ccByUsername.collect().mkString("\n"))

#对三角形进行计数

// 以规范顺序加载边和分区图
val graph = GraphLoader.edgeListFile(sc, "/home/hadoop/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)

// 寻找每个顶点对应的三角形数量
val triCounts = graph.triangleCount().vertices

// 连结用户名和三角形数量
val users = sc.textFile("/home/hadoop/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}
val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
  (username, tc)
}

// 输出结果
println(triCountByUsername.collect().mkString("\n"))

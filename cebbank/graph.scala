import org.apache.spark._
import org.apache.spark.graphx._

// 此处仍然需要用到RDD来让一些例子正常运行
import org.apache.spark.rdd.RDD

val userGraph: Graph[(String, String), String]


// 创建一个 RDD 用于表示顶点
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
// 创建一个 RDD 用于表示边
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

// 定义默认的用户，用于建立与缺失的用户之间的关系
val defaultUser = ("John Doe", "Missing")

// 构造图对象，即初始化过程
val graph = Graph(users, relationships, defaultUser)

// 统计是博士后的用户数量
graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count

// 统计符合 src > dst 条件的边的数量
graph.edges.filter(e => e.srcId > e.dstId).count

graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count

SELECT src.id, dst.id, src.attr, e.attr, dst.attr
FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
ON e.srcId = src.Id AND e.dstId = dst.Id

// 使用 triplets view 来创建一个事实（facts）的RDD
val facts: RDD[String] =
  graph.triplets.map(triplet =>
    triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
facts.collect.foreach(println(_))

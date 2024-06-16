import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GraphX SCC Example").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // 定义顶点和边
    val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
      (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E")
    ))

    val edges: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1L, 2L, 0), Edge(2L, 3L, 0), Edge(3L, 1L, 0),
      Edge(3L, 4L, 0), Edge(4L, 5L, 0), Edge(5L, 4L, 0)
    ))

    // 创建图
    val graph = Graph(vertices, edges)

    // SCC 算法
    val sccGraph = graph.stronglyConnectedComponents(5).vertices

    // 打印结果
    sccGraph.collect.foreach(println)

    sc.stop()
  }
}

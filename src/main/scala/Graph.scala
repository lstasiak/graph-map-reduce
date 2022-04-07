import org.apache.spark.rdd.RDD

class Graph(val edges: RDD[(Int, Int)]) {

  /** Class that represents directed graph The object argument is prepared graph
    * data in a form of iterable collecting tuple of two graph nodes
    * representing single connection (edge).
    */
  // SPARK VERSION
  // basic properties
  val nodes: Set[Int] = edges.keys.union(edges.values).collect().toSet

  def makeUndirected(): RDD[(Int, Int)] = {
    // returns new set of edges ignoring direction of connections
    val undirected = edges.map { case (a, b) => if (a > b) (a, b) else (b, a) }
    undirected
  }

  def degrees: RDD[(Int, Int)] = {
    /** returns RDD with node and number of nearest neighbours
      */
    val undirected = edges
      .map(tup => tup.swap)
      .union(edges)
      .reduceByKey((x, y) => x + y)

    undirected
  }

  def calculateTriangles: RDD[(Int, Int)] = {
    /**
     * Returns RDD with node and number of triangles for each node.
     * The procedure is based on MapReduce version of algorithm NodeIterator++
     */
    val flag = -1
    val undirected = makeUndirected()

    val roundOne = undirected
      .groupByKey()
      .map {
        tuple => {
          val neighboringEdges = tuple._2
            .flatMap(a => tuple._2.map(b => a -> b))
            .filterNot { case (a: Int, b: Int) => a == b }
          neighboringEdges.map(tup => (tup, tuple._1))
        }
      }
      .flatMap(x => x)
    val edgeInput = undirected.map(tuple => (tuple, flag))

    val roundTwo = roundOne
      .union(edgeInput)
      .groupByKey()
      .map(tuple => (tuple._1, tuple._2.toSet))
      .map (
        tuple => (tuple._1, tuple._2, tuple._2.contains(flag))
      )
      .filter(tuple3 => tuple3._3).map(tuple3 => tuple3._2.excl(flag))
      .map(t => {
        val neighbours: Seq[(Int, Int)] = Seq()
        for (elem <- t)
          neighbours ++ Seq((elem,1))
        neighbours
      }).flatMap(x => x).reduceByKey((x, y) => x + y)

    roundTwo
  }

  def calculateClusteringCoefficients(): RDD[(Int, Int)] = {
    /**
      Returns RDD with node and calculated local clustering coefficient for each node
      To evaluate that it is necessary to calculate number of triangles.
      */

    val triangleMapper = calculateTriangles.cache()

     val nodesDegrees = degrees.collect().toMap

     val localCoefficients = triangleMapper.map(tuple => {
       val clusteringCoefficient = 2 * tuple._2 / (nodesDegrees(tuple._1) * (nodesDegrees(tuple._1) - 1))
       (tuple._1, clusteringCoefficient)
     })

     localCoefficients
  }
}

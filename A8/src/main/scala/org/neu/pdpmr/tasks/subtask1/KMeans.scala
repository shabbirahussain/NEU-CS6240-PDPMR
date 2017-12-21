package main.scala.org.neu.pdpmr.tasks.subtask1

import org.apache.spark.rdd.RDD

/**
  * @author shabbir.ahussain
  */
class KMeans extends ClusteringAlgo {
  override def clusterValues(maxIter: Int,
                             ds: RDD[(Int, (Double, Double))]):
  RDD[((Double, Double), (Int, (Double, Double)))] = {
    val s = ds.persist.map[Double] { case (id:Int, (x: Double, y: Double)) => x }.stats()
    var newCentroids = Set((s.min, 0.0), (s.mean, 0.0), (s.max, 0.0))

    var ca = ds.map(x=>((0.0, 0.0), x)).cache()
    ds.unpersist(blocking = true)

    var i = 0
    while (true) {
      i += 1
      println("\tIter="+i)
      val centroids = newCentroids

      // Assign clusters to points
      ca = assignCentroids(ca, centroids).cache()

      // Re assign-centroids
      newCentroids = findNewCentroids(ca)

      // Check for convergence or max iterations.
      if (i > maxIter) {
        println("Max iterations reached at:" + i)
        return ca
      }
      if (newCentroids.intersect(centroids).count(x=>true) == centroids.count(x=>true)) {
        println("Convergence reached at:" + i)
        return ca
      }
    }
    null
  }

  /**
    * @param ds      is the RDD of points to cluster
    * @param centers is the seq of centorids to cluster points.
    * @return RDD of (centoids(x,y), data(x,y))
    */
  private def assignCentroids(ds: RDD[((Double, Double), (Int, (Double, Double)))],
                              centers: Set[(Double, Double)]):
  RDD[((Double, Double), (Int, (Double, Double)))] = {
    ds.map[((Double, Double), (Int, (Double, Double)))] {
      (p: ((Double, Double), (Int, (Double, Double)))) => {   //((center_x, center_y), (song_id, (x, y)))
        val tmp = centers
          .map { c =>
            (c, p._2, Math.sqrt(Math.pow(c._1 - p._2._2._1, 2) + Math.pow(c._2 - p._2._2._2, 2)))
          }
          .minBy(_._3)
        (tmp._1, tmp._2)
      }
    }
  }

  /**
    * @param rdd is the input RDD of points as (centroid(x,y), data(x,y))
    * @return Sequence of new centroids.
    */
  private def findNewCentroids(rdd: RDD[((Double, Double), (Int, (Double, Double)))]):
  Set[(Double, Double)] = {
    rdd
      .combineByKey[(Int, Double, Double, Int)](
        (v: (Int, (Double, Double))) => (v._1, v._2._1, v._2._2, 1),
        (u: (Int, Double, Double, Int), v: (Int, (Double, Double))) =>
          (u._1, u._2 + v._2._1, u._3 + v._2._2, u._4 + 1),
        (u1: (Int, Double, Double, Int), u2: (Int, Double, Double, Int)) =>
          (u1._1, u1._2 + u2._2, u1._3 + u2._3, u1._4 + u2._4))
      .map(x => (x._2._2 / x._2._4, x._2._3 / x._2._4))
      .collect()
      .toSet
  }

}

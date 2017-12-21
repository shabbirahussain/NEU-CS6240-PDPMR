package main.scala.org.neu.pdpmr.tasks.subtask1

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD

/**
  * @author shabbir.ahussain
  */
class Agglomerative extends ClusteringAlgo {
  override def clusterValues(maxIter: Int,
                             ds: RDD[(Int, (Double, Double))]): // (item_id, value)
  RDD[((Double, Double), (Int, (Double, Double)))] = {
    var dist = findDist(ds.map(x => (x._2._1, x._1)))
      .persist(StorageLevels.MEMORY_ONLY_SER_2)

    var cnt = dist.count()
    var conComp = dist
      .map[(Double, Int)](x => (x._2._1, x._2._2))
      .persist(StorageLevels.DISK_ONLY_2)

    while (cnt > 3) {
      println("\tNum Clusters = " + cnt)

      val minDist = dist.map(_._2._3).min() // Get min dist to merge

      // Update previous and next with min distance
      dist = findDist(dist
        .filter(_._2._3 != minDist)
        .map(x => (x._2._1, x._2._2)))
        .persist(StorageLevels.MEMORY_ONLY_SER_2)
      dist.checkpoint()

      cnt = dist.count()
    }


    // Compute final cluster assignment by replacing leftover distances to the original set of points.
    // Then do a groupBy and collect all points on the driver to sequentially loop through them.
    // In this design whenever we find a non infinite value (point from the un-clustered set) we get a cluster boundary.
    val res = conComp
      .leftOuterJoin(dist.map[(Double, (Int, Double))] { x => (x._2._1, (x._2._2, x._2._3)) }) // (value, (id, dist))
      .mapValues[(Int, Double)] { x: (Int, Option[(Int, Double)]) =>
        (x._1, if (x._2.isDefined) x._2.get._2 else Double.PositiveInfinity)
      }
      .sortByKey()
      .groupBy(x => 1)
      .flatMap[((Double, Double), (Int, (Double, Double)))] {
      case (_: Int, values: Iterable[(Double, (Int, Double))]) => // (_, (value, (id, distToNext)))
        var newClustStart = true
        var cid = 0.0
        values.map(v => {
          if (newClustStart) {
            cid = v._1
          }
          newClustStart = v._2._2 != Double.PositiveInfinity
          ((cid, 0.0), (v._2._1, (v._1, 0.0))) // ((cid, 0.0), (id, (value_x, 0.0)))
        })
    }
    res
  }

  /** Finds distance between consecutive points.
    *
    * @param rdd is an input RDD of (value, id)
    * @return An RDD of (index, (value, id, distance_to_next_index))
    */
  def findDist(rdd: RDD[(Double, Int)]):
  RDD[(Long, (Double, Int, Double))] = {
    val ca = rdd
      .sortByKey() // Sort by value of the point.
      .zipWithIndex()
      .map(_.swap)
      .cache()
    val ca1 = ca.map { case (idx: Long, value: (Double, Int)) => (idx - 1, value) }

    var dist = ca.leftOuterJoin(ca1)
      .mapValues { case (v1: (Double, Int), v2: Option[(Double, Int)]) =>
        (v1._1, v1._2, Math.abs(v1._1 - (if (v2.isDefined) v2.get._1 else Double.PositiveInfinity)))
      }.sortByKey()
    ca.unpersist()
    dist
  }
}

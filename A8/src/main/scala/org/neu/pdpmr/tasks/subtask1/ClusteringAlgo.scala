package main.scala.org.neu.pdpmr.tasks.subtask1

import org.apache.spark.rdd.RDD

/**
  * @author shabbir.ahussain
  */
trait ClusteringAlgo {
  /**
    * Runs clustering on given RDD
    *
    * @param maxIter is the maximum number of iterations to run.
    * @param ds      is the dataset to run clustering on. (id:Int, (x:Double, y:Double))
    * @return RDD of points to cluster assignment. (song_id, (centroid(x,y), data(x,y)))
    */
  def clusterValues(maxIter: Int, ds: RDD[(Int, (Double, Double))]):
  RDD[((Double, Double), (Int, (Double, Double)))]
}

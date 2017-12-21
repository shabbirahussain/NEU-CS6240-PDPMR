package org.neu.pdpmr.tasks.types

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SimilarArtist {
  def loadCSV(sc: SparkContext, path: String): RDD[SimilarArtist] = {
    sc.textFile(path + "similar_artists.csv.gz")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[SimilarArtist] {
      new SimilarArtist(_)
    }
  }
}

/**
  * @author shabbir.ahussain
  * @param line is the string line to parse into object.
  */
class SimilarArtist(line: String) extends Serializable {
  private val tokens = line.split(";")
  val ARTIST_ID_1: String = tokens(0)
  val ARTIST_ID_2: String = tokens(1)
}

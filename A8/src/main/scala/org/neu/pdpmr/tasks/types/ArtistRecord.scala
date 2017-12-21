package org.neu.pdpmr.tasks.types

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ArtistRecord {
  def loadCSV(sc: SparkContext, path: String): RDD[ArtistRecord] = {
    sc.textFile(path + "artist_terms.csv.gz")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[ArtistRecord] {
      new ArtistRecord(_)
    }
  }
}

/**
  * @author shabbir.ahussain
  * @param line is the string line to parse into object.
  */
class ArtistRecord(line: String) extends Serializable {
  private val tokens = line.split(";")
  val ARTIST_ID: String = tokens(0)
  val ARTIST_TERM: String = tokens(1)
  //  val ARTIST_TERM_FREQ : Double         = toSafeDouble(tokens(2), 0)
  //  val ARTIST_TERM_WEIGHT : Double       = toSafeDouble(tokens(2), 0)
}

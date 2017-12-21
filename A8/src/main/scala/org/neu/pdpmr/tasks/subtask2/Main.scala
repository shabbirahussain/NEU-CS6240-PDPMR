package main.scala.org.neu.pdpmr.tasks.subtask2

import java.io._

import main.scala.org.neu.pdpmr.tasks.Util
import org.apache.spark.rdd.RDD
import org.neu.pdpmr.tasks.subtask2.KMode
import org.neu.pdpmr.tasks.types._

/**
  * @author shabbir.ahussain
  */
object Main {
  val MostPopMax = 30
  val NumIter = 10

  /** Executes the subproblem 2 of artist clustering.
    *
    * @param outputPath is the output path to save to.
    * @param songs      is the songs RDD.
    * @param artist     is the artist RDD.
    * @param similar    is the artist similarity RDD.
    */
  def exec(outputPath: String,
           songs: RDD[SongRecord],
           artist: RDD[ArtistRecord],
           similar: RDD[SimilarArtist]) {

    val c = new KMode(MostPopMax, NumIter, outputPath)
    c.clusterValues(songs, artist, similar)

    var columns = Seq("ITER", "CLUSTER_ID", "TERM", "COUNT", "IS_CENT")
    Util.mergeFiles(new File(outputPath), columns, "clstr_term_freq-")

    columns = Seq("ITER", "CLUSTER_ID", "TERMS")
    Util.mergeFiles(new File(outputPath), columns, "centroids-")
  }

  def saveRDD(probName: String,
              fileName: String,
              ca: RDD[(String, Int)],
              at: RDD[(String, String)],
              center: RDD[(String, Int)]):
  Unit = {
    val pattern = "/clstr_term_freq-"
    val outFldr = fileName + pattern + probName
    at                                      //(artist_id, term)
      .join(ca)                             //(artist_id, (term, cluster_id))
      .map(x => (x._2, 1))                  //((term, cluster_id), 1)
      .reduceByKey(_ + _)                   //((term, cluster_id), cnt_term)
      .leftOuterJoin(center
      .map[((String, Int), Null)](x => (x, null))
    )                                       //((term, cluster_id), null)
      .map(x =>
      probName + ";" +
        x._1._2 + ";" +
        x._1._1 + ";" +
        x._2._1.intValue() + ";" +
        x._2._2.isDefined)                  // iter ; cluster_id  ; term ; freq ; matches_centroid?
      .coalesce(1, shuffle = true)
      .saveAsTextFile(outFldr)
  }

  /** Saves the centroids to centroids- file
    *
    * @param probName     is the text to include as first column in output file.
    * @param fileName     is the path of the output folder.
    * @param centroids    is the RDD of centroids to write.
    */
  def saveCentroids(probName: String,
                    fileName: String,
                    centroids: RDD[(String, Int)]):
  Unit = {
    val pattern = "/centroids-"

    centroids
      .map(_.swap)                           //(cluster_id, term)
      .map(x =>
      probName + ";" +
        x._1 + ";" +
        x._2)
      .coalesce(1, shuffle = false)
      .saveAsTextFile(fileName + pattern + probName)
  }

}

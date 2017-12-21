package org.neu.pdpmr.tasks.subtask2

import java.io.{File, FileOutputStream, PrintStream}

import org.apache.spark.SparkContext
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

/**
  * @author shabbir.ahussain
  * @param MostPopMax is the maximum number of centroids to start with.
  * @param NumIter is the max number of iterations to run.
  * @param outputPath is the output path.
  */
abstract class KClusteringBase(MostPopMax: Int, NumIter:Int, outputPath:String){
  val this.MostPopMax = MostPopMax
  val this.NumIter    = NumIter
  val this.outputPath = outputPath

  private def getTrendSetters(songs: RDD[SongRecord], similar: RDD[SimilarArtist]): Set[String] = {
    // Get artist familiarity.
    val artMaxFam = songs.map(x => (x.ARTIST_ID, x.ARTIST_FAMILIARITY))
      .combineByKey[Double]((v: Double) => v,
      (u: Double, v: Double) => Math.max(u, v),
      (u1: Double, u2: Double) => Math.max(u1, u2))

    // Get artist song count.
    val artSongCnt = songs.map(x => (x.ARTIST_ID, x.SONG_ID))
      .distinct
      .combineByKey[Double](
      (v: String) => 1.0,
      (u: Double, v: String) => u + 1,
      (u1: Double, u2: Double) => u1 + u2)

    // Get similar artist count.
    val simArtCnt = similar.map(x => (x.ARTIST_ID_1, x.ARTIST_ID_2))
      .distinct
      .combineByKey[Double](
      (v: String) => 1.0,
      (u: Double, v: String) => u + 1,
      (u1: Double, u2: Double) => u1 + u2)

    val res = artMaxFam.join(artSongCnt)
      .mapValues(x => x._1 * x._2)
      .join(simArtCnt)
      .mapValues(x => x._1 * x._2)
      .top(MostPopMax)(Ordering[Double].reverse.on(x => -x._2))
      .map(x => x._1)
      .toSet
    res
  }

  def getInitialCentroids(termArtists: RDD[(String, String)], songs: RDD[SongRecord], similar: RDD[SimilarArtist]): RDD[(String, (Int, Double))] = {

    val trendsetters = getTrendSetters(songs, similar)
    var trendTerms = termArtists
      .filter(x => trendsetters.contains(x._2))
      .map(x => ((x._1, x._2.hashCode), 1.0))                         //((term, cluster_id), score)
    val allTerms = trendTerms                                         //((term, cluster_id), score)
      .map(_._1._2)                                                   //(cluster_id)
      .distinct
      .cartesian(termArtists.map(x => x._1).distinct)                 //(term, cluster_id) -- all permutations
      .map(x => (x.swap, 0.0))                                        //((term, cluster_id), 0)

    var newCentroids = trendTerms                                     //((term, cluster_id), score)
      .union(allTerms)                                                //((term, cluster_id), score)
      .reduceByKey(_ + _)                                             //((term, cluster_id), sum_score)
      .map(x => (x._1._1, (x._1._2, x._2)))                           //(term, (cluster_id, sum_score))

    newCentroids
  }

  def saveRDD(clstrAssgn:RDD[(String, Int)],
              artistTerms:RDD[(String, String)],
              centroid: RDD[(String, (Int, Double))],
              i:Int): List[String]={
    val pattern1 = "/clstr_term_freq-"
    val outFldr1 = outputPath + pattern1 + i
    val temp = clstrAssgn                                     //(artist_id, cluster_id)
      .join(artistTerms)                                      //(artist_id, (cluster_id, term))
      .persist

    temp                                                        //(artist_id, (cluster_id, term))
      .map(x => ((x._2._1, x._2._2), 1))                        //((cluster_id, term), 1)
      .reduceByKey(_ + _)                                       //((cluster_id, term), cnt_term)
      .join(centroid                                            //(term, (cluster_id, score))
        .map[((Int, String), Double)](x=> ((x._2._1, x._1), x._2._2)) //((cluster_id, term), score)
      )                                                         //((cluster_id, term), (cnt_term, score))
      .map(x=> i + ";" +
        x._1._1 + ";" +
        x._1._2 + ";" +
        x._2._1 + ";" +
        x._2._2)                                       // iter ; cluster_id  ; term ; freq ; belongsToCentroid?
      .coalesce(1, shuffle=true)
      .saveAsTextFile(outFldr1)

    val pattern2 = "/artist_cluster_assign-"
    val outFldr2 = outputPath + pattern2 + i
    temp                                                      //(artist_id, (cluster_id, term))
      .distinct
      .map(x=> i + ";" + x._2._1 + ";" + x._1)                // i ; cluster_id ; artist_id
      .coalesce(1, shuffle=true)
      .saveAsTextFile(outFldr2)

    List(outFldr1, outFldr2)
  }

  def saveCentroids(centroids: RDD[(String, (Int, Double))],
                    ignoreScores:Boolean=false,
                    i:Int): List[String] = {
    val pattern = "/centroids-"
    val outFldr = outputPath + pattern + i

    val temp = centroids
      .map(x => (x._2._1, (x._1, x._2._2)))                               //(cluster_id, (term, score))
      .filter(x => x._2._2 > 0)
      .cache()

    temp
      .map(x=>i + ";" + x._1 + ";" + x._2._1)
      .coalesce(1, shuffle=false)
      .saveAsTextFile(outFldr)

    System.out.println("Centroids are:")
    temp
      .combineByKey[Map[String, Double]](
      (v: (String, Double)) => Map[String, Double](v._1 -> v._2),
      (u: Map[String, Double], v: (String, Double)) => u.+(v._1 -> v._2),
      (u1: Map[String, Double], u2: Map[String, Double]) => u1.++(u2))
      .sortByKey()
      .map(x=> i + ";" + x._1 + ";" + (if (ignoreScores) x._2.keySet.toSeq.sorted else x._2.toSeq.sorted))
      .foreach(println)

    List(pattern)
  }

  def exec(sc: SparkContext, songs: RDD[SongRecord], artist: RDD[ArtistRecord], similar: RDD[SimilarArtist])
}

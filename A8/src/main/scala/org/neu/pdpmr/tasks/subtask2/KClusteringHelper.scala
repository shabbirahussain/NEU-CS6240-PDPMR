package org.neu.pdpmr.tasks.subtask2

import main.scala.org.neu.pdpmr.tasks.subtask2.ClusteringAlgo
import org.apache.spark.rdd.RDD
import org.neu.pdpmr.tasks.types.{SimilarArtist, SongRecord}

/**
  * @author shabbir.ahussain
  */
class KClusteringHelper{
  /**
    * Generates initial seed centroids for clustering as per problem specifications.
    *
    * @param ta           is the RDD for artist terms.
    * @param songs        is the songs RDD.
    * @param similar      is the similar artist RDD.
    * @param numCentroids is the maximum number of centroids to start with.
    * @return An RDD of centroids (term, cluster_id).
    */
  def getInitialCentroids(ta: RDD[(String, String)],
                          songs: RDD[SongRecord],
                          similar: RDD[SimilarArtist],
                          numCentroids: Int):
  RDD[(String, Int)] = {

    println("Finding initial centroids...")
    val trendsetters = getTrendSetters(songs, similar, numCentroids)
    ta
      .filter(x => trendsetters.contains(x._2))
      .mapValues(_.hashCode)
  }

  /**
    * Gets the trendsetters as per definition of problem statement.
    *
    * @param songs      is the songs RDD.
    * @param similar    is the similar artist RDD.
    * @param mostPopMax is the maximum number of popular atrists to return.
    * @return A set of ARTIST_ID
    */
  private def getTrendSetters(songs: RDD[SongRecord],
                              similar: RDD[SimilarArtist],
                              mostPopMax: Int):
  Set[String] = {
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
      .top(mostPopMax)(Ordering[Double].reverse.on(x => -x._2))
      .map(x => x._1)
      .toSet
    res
  }
}

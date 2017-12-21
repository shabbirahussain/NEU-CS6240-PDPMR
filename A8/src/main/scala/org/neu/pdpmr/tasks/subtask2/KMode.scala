package org.neu.pdpmr.tasks.subtask2

import main.scala.org.neu.pdpmr.tasks.subtask2.{ClusteringAlgo, Main}
import org.apache.spark.rdd.RDD
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}

/**
  * @author shabbir.ahussain
  * @param MostPopMax is the maximum number of centroids to start with.
  * @param NumIter    is the max number of iterations to run.
  * @param outputPath is the output path.
  */
class KMode(MostPopMax: Int, NumIter: Int, outputPath: String) extends KClusteringHelper with ClusteringAlgo{
  val WriteFreq = 10

  /**
    * Performs clustering on given data.
    *
    * @param songs   is the RDD of songs.
    * @param artist  is the RDD of artists.
    * @param similar is the RDD of artist similarity.
    */
  def clusterValues(songs: RDD[SongRecord],
                    artist: RDD[ArtistRecord],
                    similar: RDD[SimilarArtist]):
  RDD[(String, Int)] = {

    // Clean and cache data
    val at = artist.map(x => (x.ARTIST_ID, x.ARTIST_TERM)).distinct().cache() //(artist, term)
    val ta = at.map(_.swap).cache()                     //(term, artist)

    // Get initial centroids from trend setters.
    var newCentroids = super.getInitialCentroids(ta, songs, similar, MostPopMax) //(term, cluster_id)

    var i = 0
    var continue = true
    while (continue) {
      println("\tIter="+i)
      var centroids = newCentroids.cache()              //(term, (cluster_id, score))

      // Assign points to the appropriate centroids.
      val ac = assignCentroids(ta, centroids).cache()   //(artist_id, cluster_id)

      // Reposition the centroids to the mean of the consensus.
      newCentroids = findNewCentroids(at, ac).cache()   //(term, cluster_id)


      val delta = newCentroids.subtract(centroids).count() + 1 // TODO: Soft disabled convergence criteria.
      if ((i % WriteFreq == 0) || i == (NumIter - 1) || delta == 0) {
        if (delta == 0)         {
          println("Convergence reached at:" + i)
          continue = false
        }
        if (i == (NumIter - 1)) {
          println("Max iterations reached at:" + i)
          continue = false
        }

        // Save intermediate result of each iteration.
        Main.saveRDD(i.toString, outputPath, ac, at, centroids)
        // Save centroids
        Main.saveCentroids(i.toString, outputPath, centroids)

        if (!continue) return ac
      }

      i += 1
      centroids.unpersist(blocking = false)
    }
    null
  }

  /**
    * Assign points to clusters based on intersecting term count.
    *
    * @param ta      is the RDD of (term, artist_id).
    * @param centers is the RDD of centroids of the current cluster (term, cluster_id).
    * @return A RDD of (artist_id, cluster_id).
    */
  private def assignCentroids(ta: RDD[(String, String)],
                              centers: RDD[(String, Int)]):
  RDD[(String, Int)] = {
    ta                                            //(term, artist_id)
      .join(centers)                              //(term, (artist_id, cluster_id))
      .map(x => ((x._2._1, x._2._2), 1))          //((artist_id, cluster_id), score)
      .reduceByKey(_ + _)                         //((artist_id, cluster_id), sum_score)
      .map(x => (x._1._1, (x._1._2, x._2)))       //(artist_id, (cluster_id, sum_score))
      .reduceByKey((u, v) => {                    //(artist_id, max{(cluster_id, sum_score)})
        if (u._2 > v._2) u
        else if (u._2 < v._2) v
        else if (u._1 > v._1) u                   // TIE BREAKER: if equal dist, assign to max cluster id
        else v
      })
      .map[(String, Int)](x => (x._1, x._2._1))   //(artist_id, cluster_id)
  }

  /**
    * Finds new centroids using mode for all artists in the cluster.
    *
    * @param at is the input RDD of artist terms (artist_id, term).
    * @param ac is an RDD of (artist_id, cluster_id).
    * @return RDD of new centroids (term, (cluster_id, score)).
    */
  private def findNewCentroids(at: RDD[(String, String)],
                               ac: RDD[(String, Int)]):
  RDD[(String, Int)] = {
    at                                                        //(artist_id, term)
      .join(ac)                                               //(artist_id, (term, cluster_id))
      .map(x => ((x._2._2, x._1), x._2._1))                   //((cluster_id, artist_id), term)
      .groupByKey
      .map[((Int, Int), (Seq[String], Int))](x => {
        val terms = x._2.toSet.toSeq.sorted
        ((x._1._1, terms.hashCode), (terms, 1))               //((cluster_id, terms_set_key), (Seq(term)), 1))
      })
      .reduceByKey((u1, u2) => (u1._1, u1._2 + u2._2))        //((cluster_id, terms_set_key), (Seq(term)), cnt))
      .map(x => (x._1._1, x._2))                              //(cluster_id, (Seq(term)), cnt))
      .reduceByKey((u1, u2) => if (u1._2 > u2._2) u1 else u2) //(cluster_id, max{(Seq(term), cnt)})
      .flatMap(x => x._2._1.map(y => (y, x._1)))              //(term, cluster_id)
  }
}
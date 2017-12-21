package org.neu.pdpmr.tasks.subtask2

import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author shabbir.ahussain
  * @param MostPopMax is the maximum number of centroids to start with.
  * @param NumIter is the max number of iterations to run.
  * @param outputPath is the output path.
  */
class KMode(MostPopMax: Int, NumIter:Int, outputPath:String)
  extends KClusteringBase(MostPopMax, NumIter, outputPath) {

  override def exec(sc: SparkContext, songs: RDD[SongRecord], artist: RDD[ArtistRecord], similar: RDD[SimilarArtist]){
    val artistTerms = artist
      .map(x => (x.ARTIST_ID, x.ARTIST_TERM))
      .distinct()
      .cache()

    val termArtists = artistTerms.map(_.swap).cache()                     //(term, artist)

    // Get initial centroids from trend setters.
    var newCentroids = super.getInitialCentroids(termArtists, songs, similar)   //(term, (cluster_id, score))
      .cache()
    val allTermClust = newCentroids.map(x => (x._1, (x._2._1, 0.0)))      //(term, (cluster_id, score))
      .cache()

    super.saveCentroids(newCentroids, ignoreScores = true, 0)
    for(i <- 1 to NumIter) {
      System.out.println("\nIteration " + i)
      var oldCentroids = newCentroids

      // Assign points to the appropriate centroids.
      val centXArt = termArtists                                //(term, artist_id)
        .join(oldCentroids)                                     //(term, (artist_id, (cluster_id, score)))
        .cache()

      // Pick the centroid with highest score i.e. min distance
      val clstrAssgn = centXArt                                 //(term, (artist_id, (cluster_id, score)))
        .map(x => ((x._2._1, x._2._2._1), x._2._2._2))          //((artist_id, cluster_id), score)
        .reduceByKey(_ + _)                                     //((artist_id, cluster_id), sum_score)
        .map(x => (x._1._1, (x._1._2, x._2)))                   //(artist_id, (cluster_id, sum_score))
        .reduceByKey((u, v) => {                                //(artist_id, max{(cluster_id, sum_score)})
          if (u._2 > v._2) u
          else if (u._2 < v._2) v
          else if (u._1 > v._1) u                               // if equal assign to max cluster id
          else v
        })
        .map(x => (x._1, x._2._1))                              //(artist_id, cluster_id)
        .cache()

      // Reposition the centroids to the mean of the consensus.
      val d = centXArt                                                //(term, (artist_id, (cluster_id, score)))
        .map(x => ((x._2._1, x._2._2._1), (x._1, x._2._2._2)))        //((artist_id, cluster_id), (term, score))
        .join(clstrAssgn.map[((String, Int), Null)](x => (x, null)))  //((artist_id, cluster_id), ((term, score), null))
        .mapValues(x => x._1)                                         //((artist_id, cluster_id), (term, score))
        .cache()

      newCentroids = d.mapValues(x=> x._1)                            //((artist_id, cluster_id), (term, score))
        .groupByKey
        .map[((Int, Set[String]), Int)](x => ((x._1._2, x._2.toSet), 1))  //((cluster_id, Set(term)), 1)
        .reduceByKey(_ + _)                                           //((cluster_id, Set(term)), cnt)
        .map(x=> (x._1._1, (x._1._2, x._2)))                          //(cluster_id, (Set(term), cnt))
        .reduceByKey((u1, u2)=> if (u1._2 > u2._2) u1 else u2)        //(cluster_id, max{(Set(term), cnt)})
        .flatMap(x=> x._2._1.map(y=> (y, (x._1, 1.0))))               //(term, (cluster_id, score))
        .union(allTermClust)                                          //+(term, (cluster_id, 0))
        .map(x => ((x._1, x._2._1), x._2._2))                         //((term, cluster_id), score)
        .reduceByKey(_ + _)                                           //((term, cluster_id), sum_score)
        .map(x => (x._1._1, (x._1._2, x._2)))                         //(term, (cluster_id, sum_score))

      // Save intermediate result of each iteration.
      super.saveRDD(clstrAssgn, artistTerms, oldCentroids, i)

      // Save centroids
      super.saveCentroids(newCentroids, ignoreScores = true, i)
    }
  }
}
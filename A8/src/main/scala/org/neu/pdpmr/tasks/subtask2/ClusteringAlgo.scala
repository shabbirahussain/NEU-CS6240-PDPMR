package main.scala.org.neu.pdpmr.tasks.subtask2

import org.apache.spark.rdd.RDD
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}

/**
  * @author shabbir.ahussain
  */
trait ClusteringAlgo {
  /**
    * Runs clustering on given RDD.
    *
    * @param songs   is the RDD of songs.
    * @param artist  is the RDD of artist records.
    * @param similar is the RDD of artist similarity.
    * @return RDD of points to cluster assignment. (song_id, (centroid(x,y), data(x,y)))
    */
  def clusterValues(songs: RDD[SongRecord],
                    artist: RDD[ArtistRecord],
                    similar: RDD[SimilarArtist]):
  RDD[(String, Int)]
}

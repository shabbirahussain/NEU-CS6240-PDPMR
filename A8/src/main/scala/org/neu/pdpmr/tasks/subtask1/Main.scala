package main.scala.org.neu.pdpmr.tasks.subtask1

import java.io._

import java.text.DecimalFormat

import main.scala.org.neu.pdpmr.tasks.Util
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}

/**
  * @author shabbir.ahussain
  */
object Main {
  val MaxIter = 100
  val df = new DecimalFormat("####.###")
  val pattern = "clust-"

  def exec(outputPath: String,
           songs: RDD[SongRecord],
           songsSmall: RDD[SongRecord],
           artist: RDD[ArtistRecord],
           similar: RDD[SimilarArtist]):
  Unit = {
    songs.persist(StorageLevels.MEMORY_AND_DISK_SER_2)

    println("\nExecuting artist clustering...")
    Util.timeBlock {
      this.execAlgo(outputPath + "/kmeans", songs, new KMeans(), MaxIter)
    }

    println("\nExecuting aglomerative clustering...")
    Util.timeBlock {
      println("\nCalculating fuzzy loudness (Agglomerative)...")
      this.testAgglo(outputPath + "/agglo", songs, new Agglomerative(), new KMeans, MaxIter)
    }
  }

  /**
    * Executes loudness clustering for agglomerative and kmeans.
    *
    * @param path    is the path to save the kmeans results.
    * @param songs   is the songs RDD.
    * @param algo1   is the agglomerative algorithm to use for clustering
    * @param algo2   is the kmeans algorithm to use for clustering
    * @param maxIter is the maximum number of iterations to run.
    */
  private def testAgglo(path: String,
                        songs: RDD[SongRecord],
                        algo1: Agglomerative,
                        algo2: KMeans,
                        maxIter: Int):
    Unit = {

    val testSongs = songs
      .map(x => (x.SONG_ID, x.LOUDNESS))
      .filter(_._2 != 0)
      .reduceByKey((u1, u2) => if (u1 > u2) u1 else u2)
      .map(x => (x._1.hashCode, (x._2, 0.0)))
      .cache()

    Util.timeBlock {
      saveRDD("KL", path + "/" + pattern + "0", algo2.clusterValues(MaxIter, testSongs))
    }

    Util.timeBlock {
      saveRDD("AL", path + "/" + pattern + "1", algo1.clusterValues(MaxIter, testSongs))
    }

    val columns = Seq("PROB_NAME", "CLUSTER_ID", "CLUSTER_X", "CLUSTER_Y", "X", "Y")
    Util.mergeFiles(new File(path), columns, pattern)
  }

  /**
    * Executes all clustering tasks.
    *
    * @param path    is the path to save the kmeans results.
    * @param songs   is the songs RDD.
    * @param algo    is the algorithm to use for clustering
    * @param maxIter is the maximum number of iterations to run.
    */
  private def execAlgo(path: String,
                       songs: RDD[SongRecord],
                       algo: ClusteringAlgo,
                       maxIter: Int):
  Unit = {
    // Fuzzy loudness: cluster songs into quiet, medium, and loud
    println("\nCalculating fuzzy loudness...")
    Util.timeBlock {
      saveRDD("L", path + "/" + pattern + "0",
        algo.clusterValues(maxIter,
          songs
            .map(x => (x.SONG_ID, x.LOUDNESS))
            .filter(_._2 != 0)
            .reduceByKey((u1, u2) => if (u1 > u2) u1 else u2)
            .map(x => (x._1.hashCode, (x._2, 0.0)))))
    }

    // Fuzzy length: cluster songs into short, medium, and long
    println("\nCalculating fuzzy length...")
    Util.timeBlock {
      saveRDD("N", path + "/" + pattern + "1",
        algo.clusterValues(maxIter,
          songs
            .map(x => (x.SONG_ID, x.DURATION))
            .filter(_._2 > 0)
            .reduceByKey((u1, u2) => if (u1 > u2) u1 else u2)
            .map(x => (x._1.hashCode, (x._2, 0.0)))))
    }

    // Fuzzy tempo: cluster songs into slow, medium, and fast
    println("\nCalculating fuzzy tempo...")
    Util.timeBlock {
      saveRDD("T", path + "/" + pattern + "2",
        algo.clusterValues(maxIter,
          songs
            .map(x => (x.SONG_ID, x.TEMPO))
            .filter(_._2 > 0)
            .reduceByKey((u1, u2) => if (u1 > u2) u1 else u2)
            .map(x => (x._1.hashCode, (x._2, 0.0)))))
    }

    // Fuzzy hotness: cluster songs into cool, mild, and hot based on song hotness
    println("\nCalculating fuzzy hotness...")
    Util.timeBlock {
      saveRDD("H", path + "/" + pattern + "3",
        algo.clusterValues(maxIter,
          songs
            .map(x => (x.SONG_ID, x.SONG_HOTTTNESSS))
            .filter(_._2 > 0)
            .reduceByKey((u1, u2) => if (u1 > u2) u1 else u2)
            .map(x => (x._1.hashCode, (x._2, 0.0)))))
    }

    // Combined hotness: cluster songs into cool, mild, and hot based on two dimensions:
    // artist hotness, and song hotness
    println("\nCalculating combines hotness...")
    Util.timeBlock {
      saveRDD("C", path + "/" + pattern + "4",
        algo.clusterValues(maxIter,
          songs
            .map(x => (x.SONG_ID, (x.ARTIST_HOTTTNESSS, x.SONG_HOTTTNESSS)))
            .filter(x => x._2._1 > 0 && x._2._2 > 0)
            .reduceByKey((u1, u2) => if (u1._1 > u2._1) u1 else u2)
            .map(x => (x._1.hashCode, x._2))))
    }

    println("\nFinalizing...")
    val columns = Seq("PROB_NAME", "CLUSTER_ID", "CLUSTER_X", "CLUSTER_Y", "X", "Y")
    Util.mergeFiles(new File(path), columns, pattern)
  }

  private def saveRDD(probName: String,
                      fileName: String,
                      rdd: RDD[((Double, Double), (Int, (Double, Double)))]):
  Unit = {
    rdd
      .map(x => probName      + ";" +
        x._1.hashCode()/100000+ ";" +
        df.format(x._1._1)    + ";" +
        df.format(x._1._2)    + ";" +
        df.format(x._2._2._1) + ";" +
        df.format(x._2._2._2))
      .coalesce(1, shuffle = false)
      .saveAsTextFile(fileName)
  }
}

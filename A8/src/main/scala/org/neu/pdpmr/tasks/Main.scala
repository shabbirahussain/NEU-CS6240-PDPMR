
package org.neu.pdpmr.tasks

import java.io._

import main.scala.org.neu.pdpmr.tasks.{Util, subtask1 => st1, subtask2 => st2}
import org.apache.spark.{SparkConf, SparkContext}
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}

// scalastyle:on println

/**
  * @author shabbir.ahussain
  */
object Main {
  val NumIter = 10
  var inputPath = "input/"
  var smallInputPath = "input/"
  var outputPath = "results/"

  def main(args: Array[String]) {
    // Do parameter parsing
    if (args.length > 0)
      inputPath = args(0)
    if (args.length > 1)
      outputPath = args(1)

    println("inputPath="+ inputPath)

    // Initialize
    val conf = new SparkConf()
      .setAppName("A8")
      .setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("out/temp/spark/")

    // Clean output folder
    Util.deleteRecursively(new File(outputPath))

    // Execute task
    taskA7(sc)
  }


  def taskA7(sc: SparkContext): Unit = {
    val songs = SongRecord.loadCSV(sc, inputPath)
    val artist = ArtistRecord.loadCSV(sc, inputPath)
    val similar = SimilarArtist.loadCSV(sc, inputPath)
    val songsSmall = SongRecord.loadCSV(sc, smallInputPath)

    st1.Main.exec(outputPath + "/st1", songs, songsSmall, artist, similar)
    st2.Main.exec(outputPath + "/st2", songs, artist, similar)
  }

}


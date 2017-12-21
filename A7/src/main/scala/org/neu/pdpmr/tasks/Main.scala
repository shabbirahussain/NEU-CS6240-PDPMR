
package org.neu.pdpmr.tasks

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import org.neu.pdpmr.tasks.subtask2.{KClusteringBase, KMeans, KMode}
import org.neu.pdpmr.tasks.types.{ArtistRecord, SimilarArtist, SongRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.io.Source

// scalastyle:on println

/**
 * @author shabbir.ahussain
 */
object Main {
  val DF = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")
  val MostPopMax = 30
  val NumIter = 10
  var inputPath = "input/"
  var outputPath = "results/"

  def main(args: Array[String]) {
    if (args.length > 0)
      inputPath = args(0)
    if (args.length > 1)
      outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("Task A6")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    deleteRecursively(new File(outputPath))
    taskA7Sp2(sc)

    spark.stop()
  }

  def taskA7Sp2(sc: SparkContext) {
    val songs = SongRecord.loadCSV(sc, inputPath)
    val artist = ArtistRecord.loadCSV(sc, inputPath)
    val similar = SimilarArtist.loadCSV(sc, inputPath)

    val c:KClusteringBase = new KMode(MostPopMax, NumIter, outputPath)
    c.exec(sc, songs, artist, similar)
    mergeFiles(new File(outputPath), Array[String]("ITER", "CLUSTER_ID", "TERM", "COUNT", "IS_CENT"), "clstr_term_freq-")
    mergeFiles(new File(outputPath), Array[String]("ITER", "CLUSTER_ID", "ARTIST_ID"), "artist_cluster_assign-")
    mergeFiles(new File(outputPath), Array[String]("ITER", "CLUSTER_ID", "TERMS"), "centroids-")
  }

  def timeBlock[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    println("-- Start time:" + DF.format(new Date(t0)))
    val res = block // call-by-name
    val t1 = System.currentTimeMillis()
    println("-- Elapsed time: " + (t1 - t0) / 1000 + "s")
    res
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def mergeFiles(file: File, headers:Array[String], prefix:String):Unit={
    if (!file.isDirectory) return

    val ps = new PrintStream(new FileOutputStream(file.getPath + "/" + prefix + "merged.csv"))
    ps.println(headers.mkString(";"))

    file.listFiles.foreach(d=> {
      if (d.isDirectory && d.getName.startsWith(prefix)) {
        d.listFiles.foreach(f=>{
          if (f.getName.startsWith("part-")) {
            Source.fromFile(f)
              .getLines
              .foreach(ps.println)
          }
        })
        deleteRecursively(d)
      }
    })
  }
}


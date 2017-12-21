
// scalastyle:on println
package org.neu.pdpmr.tasks

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author shabbir.ahussain
 */
object Main {
  val df = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")
  val topN = 5
  var inputFile = "input/"
  val stopList = Set("can","that","with","we","this","from","m","don","your","is","for","t","it", "my","me","to","", "s", "of", "you", "the", "i", "a", "an", "in", "on", "and", "or", "hence", "in", "below", "near")

  def main(args: Array[String]) {
    if(args.length>0)
      inputFile = args(0)

    val spark = SparkSession
      .builder()
      .appName("Task A6")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext

    val songs = loadSongCSV(sc, inputFile + "song_info.csv.gz").cache

    val time = System.currentTimeMillis()
    println("\nnumber of distinct songs")
    timeBlock {
      println("Ans:" + songs.map{_.SONG_ID}.distinct.count)
    }

    println("\nnumber of distinct artists:")
    timeBlock {
      println("Ans:" + songs.map{_.ARTIST_ID}.distinct.count)
    }

    println("\nnumber of distinct albums:")
    timeBlock {
      println("Ans:" + songs.map{x => (x.ARTIST_ID, x.RELEASE)}.distinct.count)
    }

    println("\ntop 5 loudest songs:")
    timeBlock {
      songs.top(topN)(Ordering[Double].reverse.on(x => x.LOUDNESS))
        .foreach{x => println(x.TRACK_ID, "`" + x.TITLE + "` by [" + x.ARTIST_NAME + "]", x.LOUDNESS)}
    }

    println("\ntop 5 longest songs:")
    timeBlock {
      songs.top(topN)(Ordering[Double].reverse.on(x => -x.DURATION))
        .foreach{x => println(x.TRACK_ID, "`" + x.TITLE + "` by [" + x.ARTIST_NAME + "]", x.DURATION)}
    }

    println("\ntop 5 fastest songs:")
    timeBlock {
      songs.top(topN)(Ordering[Double].reverse.on(x => -x.TEMPO))
        .foreach{x => println(x.TRACK_ID, "`" + x.TITLE + "` by [" + x.ARTIST_NAME + "]", x.TEMPO)}
    }

    println("\ntop 5 hottest songs:")
    timeBlock {
      songs.top(topN)(Ordering[Double].reverse.on(x => -x.SONG_HOTTTNESSS))
        .foreach{x => println(x.TRACK_ID, "`" + x.TITLE + "` by [" + x.ARTIST_NAME + "]", x.SONG_HOTTTNESSS)}
    }

    println("\ntop 5 hottest artists:")
    timeBlock {
      songs.groupBy{x => x.ARTIST_ID}
        .map{x => x._2.maxBy(_.ARTIST_HOTTTNESSS)}
        .top(topN)(Ordering[Double].reverse.on(x => -x.ARTIST_HOTTTNESSS))
        .foreach{x=>println(x.ARTIST_ID, x.ARTIST_NAME, x.ARTIST_HOTTTNESSS)}
    }

    println("\ntop 5 most familiar artists:")
    timeBlock {
      songs.groupBy{x => x.ARTIST_ID}
        .map{x => x._2.maxBy(_.ARTIST_FAMILIARITY)}
        .top(topN)(Ordering[Double].reverse.on(x => -x.ARTIST_FAMILIARITY))
        .foreach{x => println(x.ARTIST_ID, x.ARTIST_NAME, x.ARTIST_FAMILIARITY)}
    }

    println("\ntop 5 most popular keys (must have confidence > 0.7):")
    timeBlock {
      songs.filter{_.KEY_CONFIDENCE > 0.7}
        .map{x=>(x.KEY, 1)}
        .reduceByKey(_+_)
        .top(topN)(Ordering[Int].reverse.on(x => -x._2))
        .foreach(println)
    }

    println("\ntop 5 most prolific artists (include ex-equo items, if any):")
    timeBlock {
      songs.groupBy{x => x.ARTIST_ID}
        .map{x =>
          val res = x._2.iterator.next()
          (res.ARTIST_ID, res.ARTIST_NAME, x._2.count(x=>true))}
        .top(topN)(Ordering[Double].reverse.on(x => -x._3))
        .foreach{println}
    }

    println("\ntop 5 most common words in song titles (excluding articles, prepositions, conjunctions):")
    timeBlock {
      songs.flatMap(_.TITLE.toLowerCase.split("[^a-z]"))
        .filter(!stopList.contains(_))
        .map(word => (word, 1))
        .reduceByKey{_+_}
        .top(topN)(Ordering[Int].reverse.on(x => -x._2))
        .foreach(println)
    }

    println("\n top 5 hottest genres (mean artists hotness in artist_term):")
    val artist = loadArtistCSV(sc, inputFile + "artist_terms.csv.gz")
      .map[(String, ArtistRecord)] {x=> (x.ARTIST_ID, x)}

    timeBlock {
      songs
        .map{x => (x.ARTIST_ID, x.ARTIST_HOTTTNESSS)}
        .join(artist.mapValues{_.ARTIST_TERM})
        .map{x => (x._2._2, x._2._1)}
        .aggregateByKey[(Double, Int)]((0.0, 0))(
          (u, v) => (u._1 + v, u._2 + 1),
          (u1, u2) => (u1._1 + u2._1, u1._2 + u2._2)
        )
        .mapValues{v=>v._1/v._2}
        .top(topN)(Ordering[Double].reverse.on(x => -x._2))
        .foreach(x=> println(x._1, x._2))
    }

    spark.stop()
  }

  def loadSongCSV(sc: SparkContext, path : String):RDD[SongRecord] ={
    sc.textFile(path)
      .mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[SongRecord] {new SongRecord(_)}
  }

  def loadArtistCSV(sc: SparkContext, path : String):RDD[ArtistRecord] ={
    sc.textFile(path)
      .mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[ArtistRecord] {new ArtistRecord(_)}
  }

  def timeBlock[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    println("-- Start time:" + df.format(new Date(t0)))
    val res = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("-- Elapsed time: " + (t1 - t0)/1000 + "s")
    res
  }
}

class ArtistRecord(line: String) extends Serializable {
  private val tokens = line.split(";")
  def toSafeDouble(v : String, default : Double): Double = try{v.toDouble} catch{ case _:Exception => default}
  def toSafeInt(v : String, default : Int): Int = try{v.toDouble.toInt} catch{ case _:Exception => default}

  var ARTIST_ID : String                = tokens(0)
  var ARTIST_TERM : String              = tokens(1)
  var ARTIST_TERM_FREQ : Double         = toSafeDouble(tokens(2), 0)
  var ARTIST_TERM_WEIGHT : Double       = toSafeDouble(tokens(3), 0)
}

class SongRecord(line: String) extends Serializable {
  private val tokens = line.split(";")
  def toSafeDouble(v : String, default : Double): Double = try{v.toDouble} catch{ case _:Exception => default}
  def toSafeInt(v : String, default : Int): Int = try{v.toDouble.toInt} catch{ case _:Exception => default}

  var TRACK_ID : String                   = tokens(0)
  var AUDIO_MD5 : String                  = tokens(1)
  var END_OF_FADE_IN : String             = tokens(2)
  var START_OF_FADE_OUT : String          = tokens(3)
  var ANALYSIS_SAMPLE_RATE : String       = tokens(4)
  var DURATION : Double                   = toSafeDouble(tokens(5), 0)
  var LOUDNESS : Double                   = toSafeDouble(tokens(6), 0)
  var TEMPO : Double                      = toSafeDouble(tokens(7), 0)
  var KEY : String                        = tokens(8)
  var KEY_CONFIDENCE : Double             = toSafeDouble(tokens(9), 0)
  var MODE : String                       = tokens(10)
  var MODE_CONFIDENCE : String            = tokens(11)
  var TIME_SIGNATURE : String             = tokens(12)
  var TIME_SIGNATURE_CONFIDENCE : String  = tokens(13)
  var DANCEABILITY : String               = tokens(14)
  var ENERGY : String                     = tokens(15)
  var ARTIST_ID : String                  = tokens(16)
  var ARTIST_NAME : String                = tokens(17)
  var ARTIST_LOCATION : String            = tokens(18)
  var ARTIST_FAMILIARITY : Double         = toSafeDouble(tokens(19), 0)
  var ARTIST_HOTTTNESSS : Double          = toSafeDouble(tokens(20), 0)
  var GENRE : String                      = tokens(21)
  var RELEASE : String                    = tokens(22)
  var SONG_ID : String                    = tokens(23)
  var TITLE : String                      = tokens(24)
  var SONG_HOTTTNESSS : Double            = toSafeDouble(tokens(25), 0)
  var YEAR : Int                          = toSafeInt(tokens(26), 0)
}


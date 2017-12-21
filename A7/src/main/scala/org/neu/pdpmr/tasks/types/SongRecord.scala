package org.neu.pdpmr.tasks.types

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SongRecord{
  def loadCSV(sc: SparkContext, path : String):RDD[SongRecord] ={
    sc.textFile(path + "song_info.csv.gz")
      .mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[SongRecord] {new SongRecord(_)}
  }
}

/**
  * @author shabbir.ahussain
  * @param line is the string line to parse into object.
  */
class SongRecord(line: String) extends Serializable {
  private val tokens = line.split(";")
  private def toSafeDouble(v : String, default : Double): Double = try{v.toDouble} catch{ case _:Exception => default}
  private def toSafeInt(v : String, default : Int): Int = try{v.toDouble.toInt} catch{ case _:Exception => default}

  val TRACK_ID : String                   = tokens(0)
  val AUDIO_MD5 : String                  = tokens(1)
  val END_OF_FADE_IN : String             = tokens(2)
  val START_OF_FADE_OUT : String          = tokens(3)
  val ANALYSIS_SAMPLE_RATE : String       = tokens(4)
  val DURATION : Double                   = toSafeDouble(tokens(5), 0)
  val LOUDNESS : Double                   = toSafeDouble(tokens(6), 0)
  val TEMPO : Double                      = toSafeDouble(tokens(7), 0)
  val KEY : String                        = tokens(8)
  val KEY_CONFIDENCE : Double             = toSafeDouble(tokens(5), 0)
  val MODE : String                       = tokens(10)
  val MODE_CONFIDENCE : String            = tokens(11)
  val TIME_SIGNATURE : String             = tokens(12)
  val TIME_SIGNATURE_CONFIDENCE : String  = tokens(13)
  val DANCEABILITY : String               = tokens(14)
  val ENERGY : String                     = tokens(15)
  val ARTIST_ID : String                  = tokens(16)
  val ARTIST_NAME : String                = tokens(17)
  val ARTIST_LOCATION : String            = tokens(18)
  val ARTIST_FAMILIARITY : Double         = toSafeDouble(tokens(19), 0)
  val ARTIST_HOTTTNESSS : Double          = toSafeDouble(tokens(20), 0)
  val GENRE : String                      = tokens(21)
  val RELEASE : String                    = tokens(22)
  val SONG_ID : String                    = tokens(23)
  val TITLE : String                      = tokens(24)
  val SONG_HOTTTNESSS : Double            = toSafeDouble(tokens(25), 0)
  val YEAR : Int                          = toSafeInt(tokens(26), 0)
}
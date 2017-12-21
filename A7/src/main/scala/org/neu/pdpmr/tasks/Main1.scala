package org.neu.pdpmr.tasks

// Author: Sharad
import java.lang.Math.{pow, sqrt, abs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import org.neu.pdpmr.tasks.types.SongRecord

case class Point1d(id: String, x: Double) {
	def delta(other: Point1d) = abs(this.x - other.x)
}

case class Point2d(id: String, x: Double, y:Double) {
	def delta(other: Point2d) = sqrt( pow((this.x - other.x),2) + pow((this.y - other.y),2) )
}

object Main1 {

	def main(args: Array[String]) {


		val conf = new SparkConf().setMaster("local").setAppName("Sub Part 1")
				val sc = new SparkContext(conf)
				val inputPath = args(0)
				val k: Int = 3
				val iters: Int = 10
				val songs = SongRecord.loadCSV(sc, inputPath)

				// loudness
				val loudness_pts = songs.map(x => Point1d(x.SONG_ID ,x.LOUDNESS)).cache
				var centers = loudness_pts.take(3).toList
				val loudClusterKmeans = writeOutput(makeCluster1d(centers,loudness_pts,iters,sc),"loud")
				loudness_pts.unpersist()
				// length
				val duration_pts = songs.map(x => Point1d(x.SONG_ID ,x.DURATION) ).cache
				centers = duration_pts.take(3).toList
				val lengthClusterKmeans = writeOutput(makeCluster1d(centers,duration_pts,iters,sc),"length")
				duration_pts.unpersist()
				// tempo
				val tempo_pts = songs.map(x => Point1d(x.SONG_ID ,x.TEMPO) ).cache
				centers = tempo_pts.take(3).toList
				val tempoClusterkmeans = writeOutput(makeCluster1d(centers,tempo_pts,iters,sc),"tempo")
				tempo_pts.unpersist()
				// hotness
				val hot_pts = songs.map(x => Point1d(x.SONG_ID ,x.SONG_HOTTTNESSS) ).cache
				centers = hot_pts.take(3).toList
				val hotClusterKmeans = writeOutput(makeCluster1d(centers,hot_pts,iters,sc),"hot")
				hot_pts.unpersist()
				// combined hotness
				val hotness_pts = songs.map(x => Point2d(x.SONG_ID ,x.ARTIST_HOTTTNESSS,x.SONG_HOTTTNESSS)).cache
				val centers2d = hotness_pts.take(3).toList
				val hotnessClusterKmeans = writeOutput2d(makeCluster2d(centers2d,hotness_pts,iters,sc),"comhot")
				hotness_pts.unpersist()



	}
	def writeOutput(rdd : RDD[((Point1d,Iterable[Point1d]),Long)],s:String) {
		var low = rdd.filter(_._2==0).map(_._1).map(_._2).flatMap(i => i.toList)
				var med = rdd.filter(_._2==1).map(_._1).map(_._2).flatMap(i => i.toList)
				var high = rdd.filter(_._2==2).map(_._1).map(_._2).flatMap(i => i.toList)
				low.map(r => (r.id,r.x))
				  .map({ case (str, int) => s"$str,$int"})
				  .saveAsTextFile("output/"+s+"/low")
				med.map(r => (r.id,r.x))
				  .map({ case (str, int) => s"$str,$int"})
				  .saveAsTextFile("output/"+s+"/med")
				high.map(r => (r.id,r.x))
				  .map({ case (str, int) => s"$str,$int"})
				  .saveAsTextFile("output/"+s+"/high")
	}

	def writeOutput2d(rdd : RDD[((Point2d,Iterable[Point2d]),Long)],s:String) {
		var low = rdd.filter(_._2==0).map(_._1).map(_._2).flatMap(i => i.toList)
				var med = rdd.filter(_._2==1).map(_._1).map(_._2).flatMap(i => i.toList)
				var high = rdd.filter(_._2==2).map(_._1).map(_._2).flatMap(i => i.toList)
				low.map(r => (r.id,r.x,r.y))
				  .map({ case (str, int1,int2) => s"$str,$int1,$int2"})
				  .saveAsTextFile("output/"+s+"/low") 
				med.map(r => (r.id,r.x,r.y))
				  .map({ case (str, int1,int2) => s"$str,$int1,$int2"})
				  .saveAsTextFile("output/"+s+"/med")
				high.map(r => (r.id,r.x,r.y))
				  .map({ case (str, int1,int2) => s"$str,$int1,$int2"})
				  .saveAsTextFile("output/"+s+"/high")
	}
        // Author: Ankur Dave
	def findClosestCenter(centers: List[Point1d],point: Point1d) = {
			centers.reduce( (i,j) => if ((point delta i) > (point delta j)) j else i )
	}
        //
	def findClosestCenter2d(centers: List[Point2d],point: Point2d) = {
			centers.reduce( (i,j) => if ((point delta i) > (point delta j)) j else i )
	}  

	def makeCluster1d(centers: List[Point1d],pts: RDD[Point1d],iters: Int,sc: SparkContext): RDD[((Point1d,Iterable[Point1d]),Long)] = {
			val clusters = pts.map( point => findClosestCenter(centers,point) -> point).groupByKey
					if(iters==1)
						return clusters.zipWithIndex
								val nextCenters = centers.map(prevCenter => {
									clusters.map(_._2).take(1)
								})
								makeCluster1d(centers,pts,iters-1,sc)
	} 

	def makeCluster2d(centers: List[Point2d],pts: RDD[Point2d],iters: Int,sc: SparkContext) : RDD[((Point2d,Iterable[Point2d]),Long)]= {
			val clusters = pts.map( point => findClosestCenter2d(centers,point) -> point)
					.groupByKey
					if(iters==1)
						return clusters.zipWithIndex

								val nextCenters = centers.map(prevCenter => {
									clusters.map(_._2).take(1)
								}).toList
								makeCluster2d(centers,pts,iters-1,sc)
	}

}

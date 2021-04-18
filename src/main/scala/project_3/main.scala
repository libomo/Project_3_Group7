package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}


/*
              Testing verifyMIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ../project_3_data/small_edges_MIS.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ../project_3_data/small_edges_non_MIS.csv

              Testing small_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/small_edges.csv ourResults/our_small_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_small_edges_MIS/aaa.csv

              Testing line_100_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/line_100_edges.csv ourResults/our_line_100_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_line_100_edges_MIS/aaa.csv

              Testing twitter_10000_edges.csv
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute ../project_3_data/twitter_10000_edges.csv ourResults/our_twitter_10000_edges_MIS
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify ../project_3_data/small_edges.csv ourResults/our_twitter_10000_edges_MIS/aaa.csv


Results are all good so far !!!
*/


object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id, vd) => (0.asInstanceOf[Int], 0.asInstanceOf[Float]))
    var remaining_vertices = 2.asInstanceOf[Long]
    val r = scala.util.Random
    var iteration = 0
    while (remaining_vertices >= 1) {
      // To Implement
      iteration += 1
      g = g.mapVertices((id, vd) => (vd._1, r.nextFloat()))
      val v = g.aggregateMessages[(Int, Float)](
        e => { 
          e.sendToDst(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (0, 0) else (1, 0)); 
          e.sendToSrc(if ((e.srcAttr._2 + e.srcAttr._1) > (e.dstAttr._2 + e.dstAttr._1)) (1, 0) else (0, 0)) 
        }, 
        (msg1, msg2) => if (msg1._1 == 1 && msg2._1 == 1) (1, 0) else (0, 0)
      )

      val g2 = Graph(v, g.edges)
      
      val v2 = g2.aggregateMessages[(Int, Float)](
        e => {
          e.sendToDst(if (e.dstAttr._1 == 1) (1, 0) else (if (e.srcAttr._1 == 1) (-1, 0) else (0, 0))); 
          e.sendToSrc(if (e.srcAttr._1 == 1) (1, 0) else (if (e.dstAttr._1 == 1) (-1, 0) else (0, 0))) 
        }, 
        (msg1, msg2) => if (msg1._1 == 1 || msg2._1 == 1) (1, 0) else (if (msg1._1 == -1 || msg2._1 == -1) (-1, 0) else (0, 0))
      )

      g = Graph(v2, g.edges)
      g.cache()
      remaining_vertices = g.vertices.filter({case (id, x) => (x._1 == 0)} ).count()
    }
    println("************************************************************")
    println("Total number of Iteration = " + iteration)
    println("************************************************************")
    return g.mapVertices((id, vd) => (vd._1))
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // To Implement
    // def msgFun(triplet: EdgeContext[Int, Int, Int]) {
    //   triplet.sendToDst("Hi")}
    // {e.sendToSrc(1,e.dstAttr); e.sendToDst(1,e.srcAttr)}, reduceFun

    val my = g_in.aggregateMessages[Int](
      e => {
        e.sendToDst(if (e.dstAttr == 1 && e.srcAttr == 1) 2 else (if (e.dstAttr == -1 && e.srcAttr == -1) -1 else 0)); 
        e.sendToSrc(if (e.dstAttr == 1 && e.srcAttr == 1) 2 else (if (e.dstAttr == -1 && e.srcAttr == -1) -1 else 0))
      },
      (msg1, msg2) => if (msg1 == 2 || msg2 == 2) 2 else (if (msg1 == -1 && msg2 == -1) -1 else 0)
    )
    val count2 = my.filter(x => x._2 == 2).count()
    val count_1 = my.filter(x => x._2 == -1).count()

    return count2 == 0 && count_1 == 0

    // val edges = g_in.edges
    // val count = edges.map(e => {if(e.dstAttr == 1 && e.srcAttr == 1) false else true}).filter(x => x == false).count()
    // if (count > 0) return false


    // def reduceFun(a: (Int, Int), b: (Int,Int)) : (Int, Int) = (a._1 + b._1, a._2 + b._2)
    // val ret = g_in.aggregateMessages[(Int, Int)](e => 
    // if(e.dstAttr == -1) e.sendToDst(1,e.scrAttr)
    // else if (e.dstAttr == 1) e.sendToDst(0,0) 
    // else if (e.srcAttr == -1) e.sendToSrc(1,e.dstAttr) 
    // else if (e.srcAttr == 1) e.sendToSrc(0,0), reduceFun _)
    
    // val ans = ret.filter(x => x._2._1 != 0).map(x => if(x._2._1 == x._2._2*-1) false else true).filter(x=> x == false).count()
    // return ans==0
  }






  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}

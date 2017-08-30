package pub.ayada.scala.sparkUtils.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.storage.StorageLevel

import pub.ayada.scala.sparkUtils.etl.read.ReadProps
import pub.ayada.scala.sparkUtils.etl.read.jdbc.{ Jdbc2DF, Jdbc2DFProps }
import pub.ayada.scala.sparkUtils.etl.read.hive.{ Hive2DF, Hive2DFProps }
import pub.ayada.scala.sparkUtils.etl.read.csv.{ Csv2DF, Csv2DFProps }

import pub.ayada.scala.sparkUtils.etl.write.WriteProps
import pub.ayada.scala.sparkUtils.etl.write.hive.{ DF2Hive, DF2HiveProps }
import pub.ayada.scala.sparkUtils.etl.write.parquet.{ DF2Parquet, DF2ParquetProps }

import pub.ayada.scala.sparkUtils.etl.transform.{ Transformer, TransformerDFs, TransformerProps }

import pub.ayada.scala.utils.DateTimeUtils

class Executor(sparkContext : SparkContext,
               jobProps : JobProps,
               defaults : scala.collection.mutable.Map[String, String],
               status : scala.collection.mutable.HashMap[String, Int]) {
    //private lazy val pool = java.util.concurrent.Executors.newFixedThreadPool(maxThread)
    val jobId = "Executor-" + jobProps.id
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.udf
    import sqlContext.sql
    import sqlContext.implicits._
    import org.apache.spark.sql._

    val hiveContext = new HiveContext(sparkContext)
    import hiveContext.sql
    import hiveContext.implicits._

    def process() {
        try {
            var dfs : scala.collection.mutable.Map[String, TransformerDFs] = scala.collection.mutable.Map()

            for (r <- jobProps.readProps) {
                r._1.toLowerCase match {
                    case "jdbc" => {
                        new Jdbc2DF(jobProps.id, sqlContext, hiveContext, defaults, r._2.asInstanceOf[Jdbc2DFProps], dfs).execute
                    }
                    case "hive" => {
                        new Hive2DF(jobProps.id, hiveContext, defaults, r._2.asInstanceOf[Hive2DFProps], dfs).execute
                    }
                    case "csv" => {
                        new Csv2DF(jobProps.id, hiveContext, defaults, r._2.asInstanceOf[Csv2DFProps], dfs).execute
                    }
                    case _ =>
                        println(DateTimeUtils.getFmtDtm(jobId) + "Err:Task ignored as the associated load type is currently not supported. Type:" + r.toString())
                }
            }
            /*
            pool.shutdown();
            while (!pool.isTerminated()) {
                Thread.sleep(5000)
            }
*/

            /*            var tup : (String, TransformerDFs) = null
            for (r <- jobProps.readProps) {
                var transformerDFs = dfs.getOrElse(r._2.id, null)
                if (null != transformerDFs)
                    transformerDFs.df.registerTempTable(r._2.id)
            }
*/
            if (dfs.size > 0) {
                var pending : scala.collection.mutable.ListBuffer[TransformerProps] = Transformer.runTransforms(jobProps.id, hiveContext, jobProps.transformProps, defaults, dfs)
                while (pending.size > 0) {
                    pending = Transformer.runTransforms(jobProps.id, hiveContext, pending.toList, defaults, dfs)
                }

                println(DateTimeUtils.getFmtDtm(jobId) + "List of DFs: " + dfs.keySet.mkString(",") + "\n")
                storeAndUnpersitDFs(dfs, jobProps.writeProps)
                unpersitDFs(dfs, jobProps.transformProps)
            } else {
                println(DateTimeUtils.getFmtDtm(jobId) + "Err: Nothing to load as no DataFrames were created.")
            }

        } catch {
            case e : Exception =>
                val sb = new StringBuilder(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(jobId))
                sb.append(e).append("\nStackTrace:\n")
                for (element : StackTraceElement <- e.getStackTrace()) {
                    sb.append(element.toString());
                    sb.append("\n");
                }
                println(sb.toString)
                status.put(jobProps.id, 1)
        } finally {
            println(DateTimeUtils.getFmtDtm(jobId) + "Processing ended")
            Thread.`yield`
        }
    }

    def storeAndUnpersitDFs(dfs : scala.collection.mutable.Map[String, TransformerDFs],
                            writeProps : List[(String, WriteProps)]) {
        println(DateTimeUtils.getFmtDtm(jobId) + "Starting Hive load. Number tables to load:" + writeProps.size)
        writeProps.foreach(hp => {
            println(DateTimeUtils.getFmtDtm(jobId + "-" + hp._2.srcDF) + "Preparing for hive load id:" + hp._2.id + ", Source DF:" + hp._2.srcDF)

            val d : TransformerDFs = dfs.getOrElse(hp._2.srcDF, null)
            if (d.df == null) {
                println(DateTimeUtils.getFmtDtm(jobId + "-" + hp._2.srcDF) + "Err: '" + hp._2.srcDF + "' is null. Not writing to destination: " + hp._1)
            } else {
                hp._2 match {
                    case DF2HiveProps(taskType, id, srcDF, loadType, schema, table, format, partitionColumns, preLoadCount, postLoadCount) =>
                        DF2Hive.write2Hive(id, hiveContext, hp._2.asInstanceOf[DF2HiveProps], d)
                    case DF2ParquetProps(taskType, id, srcDF, loadType, fileDir, partitionColumns, preLoadCount, postLoadCount) =>
                        DF2Parquet.write2Parquet(id, hiveContext, hp._2.asInstanceOf[DF2ParquetProps], d)
                }
                d.loadCount = false
            }
        })
    }

    def unpersitDFs(dfs : scala.collection.mutable.Map[String, TransformerDFs],
                    transformProps : List[TransformerProps]) {
        println(DateTimeUtils.getFmtDtm(jobId) + "Starting to unpersist DataFrames")

        for (k <- dfs.keys) {
            val df : TransformerDFs = dfs.getOrElse(k, null)
            if (df != null & df.df != null) {
                try {
                    println(DateTimeUtils.getFmtDtm(jobId) + "Unpersisting: " + k)
                    df.df.unpersist()
                } catch { case t : Throwable => }
            }

        }
    }
}

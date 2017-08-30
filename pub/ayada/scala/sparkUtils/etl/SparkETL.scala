package pub.ayada.scala.sparkUtils.etl
import java.time.Duration
import scala.xml.Elem

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import pub.ayada.scala.utils.hdfs.HdfsFileIO
import pub.ayada.scala.utils.DateTimeUtils
import pub.ayada.scala.utils.ObjectUtils

import pub.ayada.scala.sparkUtils.etl.SparkETL._
import pub.ayada.scala.utils.DateTimeUtils._

class SparkETL(args : Array[String]) {

    private lazy val argsMap : scala.collection.mutable.Map[String, String] = {
        val map : scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
        args.sliding(2, 1).toList.collect { case Array(key : String, value : String) => map.put(key, value) }
        map
    }

    // xml DOM object of the input xml file. Used for running the xpath queries
    private lazy val jobConfXML : scala.xml.Elem = {
        val xmlPath = argsMap.getOrElse("--JobConf", null)
        if ("" == xmlPath) {
            throw new java.lang.Exception(DateTimeUtils.getFmtDtm("Driver-Main") + "Failed to parse the commanline. Need the parameter --JobConf \"<path to xml file>\" ")
        }
        println(DateTimeUtils.getFmtDtm("Driver-Main") + "--JobConf: " + xmlPath)
        val xml : scala.xml.Elem = scala.xml.XML.load(new java.io.InputStreamReader(HdfsFileIO.getHdfsFlInputStream(xmlPath), "UTF-8"))
        if (xml == null) {
            throw new java.lang.Exception(DateTimeUtils.getFmtDtm("Driver-Main") + "Failed to parse the commanline. Need the parameter --JobConf \"<path to xml file>\" ")
        }
        xml
    }

    private lazy val AppName = (jobConfXML \ "@appName").text
    private lazy val defaults : scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

    /*
     * Spark Context
     */
    private lazy val sparkContext : SparkContext = {
        val cnf = new SparkConf()
        cnf.setAppName(AppName)
            .set("spark.scheduler.mode", "FAIR")
            .set("spark.sql.hive.convertMetastoreParquet", "false")
            .set("spark.sql.parquet.mergeSchema", "false")
            .set("spark.sql.parquet.int96AsTimestamp", "true")
            .set("spark.shuffle.service.enabled", "true")
            .set("hive.execution.engine", "spark")
            .set("spark.driver.allowMultipleContexts", "false")
            .set("spark.rpc.io.serverThreads", "32")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("yarn.nodemanager.vmem-check-enabled", "false")
        if (argsMap.contains("fs.s3a.access.key") && argsMap.contains("fs.s3a.secret.key")) {
            cnf.set("fs.s3a.access.key", argsMap.getOrElse("fs.s3a.access.key", null))
                .set("fs.s3a.secret.key", argsMap.getOrElse("fs.s3a.secret.key", null))
        }
        val sc = new org.apache.spark.SparkContext(cnf)
        sc.setLogLevel("ERROR")
        sc
    }
    defaults.put("defaultNoOfDFPartitions", {
        if ("" != (jobConfXML \ "@defaultNoOfPartitions").text.trim)
            (jobConfXML \ "@defaultNoOfPartitions").text.trim
        else {
            {
                sparkContext.getConf.getInt("spark.executor.instances", 5) *
                    sparkContext.getConf.getInt("spark.executor.cores", 1) * 2
            }.toString
        }
    })

    defaults.put("defaultStorageLevel", {
        if ("" != (jobConfXML \ "@defaultStorageLevel").text.trim)
            (jobConfXML \ "@defaultStorageLevel").text.trim
        else
            "MEMORY_AND_DISK"
    })

    //List of properties passed in the input properties file provided in the config file
    private lazy val jobProps : List[JobProps] = {

        var jobProps : scala.collection.mutable.ListBuffer[JobProps] = scala.collection.mutable.ListBuffer()
        val printer = new scala.xml.PrettyPrinter(250, 4)

        (jobConfXML \ "job").foreach(job => {

            val jobId = (job \ "@id").text.trim

            val sb = new StringBuilder()
            sb.append(DateTimeUtils.getFmtDtm("Driver-Main")).append(" Job: ").append(jobId).append("\n")

            sb.append(DateTimeUtils.getFmtDtm("Driver-Main")).append(" Input Config:\n").append(printer.format(job))

            val WriteProps = JobProps.getDF2TargetProps((job \ "target"), sparkContext, defaults, sb)

            // If we failed to determine at least one Target, no point in proceeding with the further processing.
            if (WriteProps.size == 0) {
                println(DateTimeUtils.getFmtDtm("Driver-Main") + "Err: Failed to retrive properties to persit final data in Hive. Skippig the ETL. JobID:" + jobId)
            } else {
                //retrieve and report Source properties
                val src2DFProps = JobProps.getSrc2DFProps((job \ "src"), sparkContext, defaults, sb)
                sb.append("\n\n").append(DateTimeUtils.getFmtDtm("Driver-Main") + "Read Tasks of job: " + jobId)
                for (pp <- src2DFProps) {
                    sb.append("\n\t\t")
                        .append("Source Type: ")
                        .append(pp._1)
                        .append(" -> ")
                        .append("Property: ")
                        .append(pp._2.toString)
                }

                //retrieve and report  Transformer properties
                val transformerProps = JobProps.getTransformers((job \ "transformers"), defaults)
                sb.append("\n")
                    .append(DateTimeUtils.getFmtDtm("Driver-Main"))
                    .append("Transformation Properties for job: " + jobId)
                for (tt <- transformerProps) {
                    sb.append("\n\t\t")
                        .append(tt.toString)
                }

                //report Target Properties
                sb.append("\n").append(DateTimeUtils.getFmtDtm("Driver-Main"))
                    .append("Target Properties for job: " + jobId)
                for (hh <- WriteProps) {
                    sb.append("\n\t\t")
                        .append(ObjectUtils.prettyPrint(hh))
                }

                jobProps += JobProps(id = jobId, readProps = src2DFProps, writeProps = WriteProps, transformProps = transformerProps)
            }
            sb.append("\n")
            println(sb.toString)
        })

        println(DateTimeUtils.getFmtDtm("Driver-Main") + "# jobs to process: " + jobProps.size)
        jobProps.toList
    }

    /**
      * Starts off the load process.
      */
    def kickoffLoad() : Int = {
        if (sparkContext == null) {
            throw new Exception("Spark Context is not yet initialized... :(")
        }

        val status = new scala.collection.mutable.HashMap[String, Int]()
        jobProps.foreach { p =>
            println(DateTimeUtils.getFmtDtm("Driver-Main") + "Processing JobID : " + p.id)
            status.put(p.id, 0)
            val j = new Executor(sparkContext, p, defaults, status)
            j.process()
        }

        var rc = 0
        for ((k, v) <- status) {
            println(DateTimeUtils.getFmtDtm("Driver-Main") + "RC for job : " + k + " is : " + v)
            if (0 != v) rc += v
        }
        println(DateTimeUtils.getFmtDtm("Driver-Main") + "Final RC is : " + rc)
        rc
    }
}

object SparkETL {
    def main(args : Array[String]) : Unit = {
        val run = args(0)
        var rc = 0
        val sparkETL : SparkETL = new SparkETL(args)
        rc = sparkETL.kickoffLoad

        if (rc != 0)
            throw new Exception("Failed to complete all the tasks..")
    }
}

/*

spark-shell --packages com.databricks:spark-csv_2.10:1.5.0
val hc = new org.apache.spark.sql.hive.HiveContext(sc)
val dfOptions: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]("header" -> "true", "delimiter" -> "|", "quote" -> "\"","escape" -> "\\","mode" -> "PERMISSIVE","charset" -> "UTF-8","comment" -> null,"dateFormat" ->  "M/d/yyyy H:m:s a","nullValue" -> "","inferSchema" -> "true")
val inDF = hc.read.format("com.databricks.spark.csv").options(dfOptions).load("/user/krnydc/Bounces.txt")
inDF.registerTempTable("inDF")
inDF.printSchema

 */

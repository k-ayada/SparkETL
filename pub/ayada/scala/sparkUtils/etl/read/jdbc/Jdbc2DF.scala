package pub.ayada.scala.sparkUtils.etl.read.jdbc

import scala.xml.Node
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import pub.ayada.scala.utils.DateTimeUtils

import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs
import pub.ayada.scala.sparkUtils.cmn.UtilFuncs

import pub.ayada.scala.sparkUtils.etl.read.ReadProps
import pub.ayada.scala.sparkUtils.etl.read.Common

class Jdbc2DF(jobID : String,
              sqlContext : SQLContext,
              hiveContext : HiveContext,
              defaults : scala.collection.mutable.Map[String, String],
              props : pub.ayada.scala.sparkUtils.etl.read.jdbc.Jdbc2DFProps,
              dfs : scala.collection.mutable.Map[String, TransformerDFs]) {

    val dfOptions = props.connParms

    var lowerBound = props.lowerBound
    var upperBound = props.upperBound
    val partKey = props.partitionKey.toUpperCase
    val noOfPartitions = props.noOfPartitions
    val taskID = "Jdbc2DF-" + jobID + "-" + props.id

    def execute() = {
        import sqlContext.sql
        import sqlContext.implicits._
        dfOptions += ("dbtable" -> { "(" + props.sql + ") tmp" })
        dfOptions += ("fetchsize" -> props.fetchSize)
        dfOptions += ("isolationLevel" -> props.isolationLevel)

        if ("" == partKey || "NONE" == partKey) {
            println(DateTimeUtils.getFmtDtm(taskID) + "No partition info found. Running single JDBC import")
        } else {
            if ("" == lowerBound || "" == upperBound) {
                val boundSql : StringBuilder = new StringBuilder("(select")
                boundSql.append("  min(").append(partKey).append(") as minKey ")
                    .append(", max(").append(partKey).append(") as maxKey ")
                    .append(" from ").append("(" + props.sql + ")) tmp")
                println(DateTimeUtils.getFmtDtm(taskID) + "Retrieving  the lower and upper bounds using the SQL: " + boundSql.toString)
                var dframeBounds = sqlContext.read.format("jdbc")
                    .options(dfOptions)
                    .option("dbtable", boundSql.toString)
                    .load()

                //lb = BigInt(dframeBounds.map(t => t(0)).collect()(0).asInstanceOf[java.math.BigDecimal].toBigInteger())
                //ub = BigInt(dframeBounds.map(t => t(1)).collect()(0).asInstanceOf[java.math.BigDecimal].toBigInteger())

                val (l, u) = dframeBounds.map(t => (t(0), t(1))).collect()(0)
                lowerBound = BigInt(l.asInstanceOf[(java.math.BigDecimal)].toBigInteger).toString
                upperBound = BigInt(u.asInstanceOf[(java.math.BigDecimal)].toBigInteger).toString
            }

            dfOptions += ("partitionColumn" -> partKey)
            if ("" != lowerBound && "" != upperBound) {
                println(DateTimeUtils.getFmtDtm(taskID) + "Forcing lowerBound:" + lowerBound + " and upperBound: " + upperBound)
                dfOptions += ("lowerBound" -> lowerBound)
                dfOptions += ("upperBound" -> upperBound)
            }

            if ("" != noOfPartitions) {
                dfOptions += ("numPartitions" -> noOfPartitions)
            } else {
                dfOptions += ("numPartitions" -> defaults.getOrElse("defaultNoOfPartitions", "2"))
                println(DateTimeUtils.getFmtDtm(taskID) + "Forcing nmPartitions:" + dfOptions.get("numPartitions").get)
            }

        }

        println(DateTimeUtils.getFmtDtm(taskID) + "All Props:" + props.toString)
        val dfo = for (k <- dfOptions.keys) yield {
            if ("password" == k) { k + " -> ********" }
            else { k + " -> " + dfOptions.getOrElse(k, "") }
        }
        println(DateTimeUtils.getFmtDtm(taskID) + "DataFrame options: " + dfo.mkString(" , "))
        println(DateTimeUtils.getFmtDtm(taskID) + "Building DataFrame")
        import hiveContext.sql
        import hiveContext.implicits._

        try {
            val (df, msg) = Common.getTransformerDF(taskID, defaults, props, hiveContext.read.format("jdbc").options(dfOptions).load())
            dfs.put(props.id, df)
            df.df.registerTempTable(props.id)
            println(DateTimeUtils.getFmtDtm(taskID) + msg)
            if (props.printSchema) {
                println(DateTimeUtils.getFmtDtm(taskID) + "DF schema of : " + props.id + "\n" + df.df.schema.treeString)
            }

        } catch {
            case t : Exception =>
                println(DateTimeUtils.getFmtDtm(taskID) + "Failed to process JDBC read task.")
                t.printStackTrace()
                throw new Exception(t)
        }
    }

}

object Jdbc2DF {
    /**
      * extracts the JDBC jobinfo from the input xml node object. If a property is
      * defined in both propsFile and in the xml, the value in the xml will take precedence
      * <br>List of properties, <code>
      * <br>"jdbc" \ "@propsFile" &nbsp;&nbsp; (default:null)
      * <br>"jdbc" \ "@schema" &nbsp;&nbsp; (default:null)
      * <br>"jdbc" \ "@table" &nbsp;&nbsp; (default:null)
      * <br>"jdbc" \ "@fetchSize" &nbsp;&nbsp; (default: "100000")
      * <br>"jdbc" \ "@isolationLevel" &nbsp;&nbsp; (default:"READ_COMMIT")
      * <br>"jdbc" \ "@loadCount" &nbsp;&nbsp; (default: "false")
      * <br>"jdbc" \ "@printSchema" &nbsp;&nbsp; (default: "false")
      * <br>"jdbc" \ "sql" &nbsp;&nbsp; (default:null)
      * <br>"jdbc" \ "@partitionKey" &nbsp;&nbsp; (default:null)
      * <br>"jdbc" \ "@noOfPartitions" &nbsp;&nbsp; (default:null if @partitionKey is null else 5)
      * <br>"jdbc" \ "@lowerBound" &nbsp;&nbsp; (default:null if @partitionKey  else calculated as min( if @partitionKey)
      * <br>"jdbc" \ "@upperBound" &nbsp;&nbsp; (default:null if @partitionKey else calculated as max( if @partitionKey))
      * <code>
      */
    def getJdbc2DFProp(jobxmlNode : Node,
                       sc : org.apache.spark.SparkContext,
                       defaults : scala.collection.mutable.Map[String, String],
                       logs : StringBuilder) : pub.ayada.scala.sparkUtils.etl.read.jdbc.Jdbc2DFProps = {
        var JobProps : java.util.Properties = new java.util.Properties
        val JDBCPropsFile = (jobxmlNode \ "@propsFile").text.trim
        val JDBCId = (jobxmlNode \ "@id").text.trim
        val JDBCSchema = (jobxmlNode \ "@schema").text.trim
        val JDBCTable = (jobxmlNode \ "@table").text.trim
        val JDBCFetchSize = (jobxmlNode \ "@fetchSize").text.trim
        val JDBCIsolationLevel = (jobxmlNode \ "@isolationLevel").text.trim
        val JDBCRepartition = (jobxmlNode \ "@repartition").text.trim
        val JDBCLoadCount = (jobxmlNode \ "@loadCount").text.trim.toLowerCase
        val JDBCPrintSchema = (jobxmlNode \ "@printSchema").text.trim.toLowerCase

        val Repartition = (jobxmlNode \ "@repartition").text.trim.toLowerCase
        val Broadcast = (jobxmlNode \ "@broadcast").text.trim.toLowerCase

        val PartitionKey = (jobxmlNode \ "@partitionKey").text.trim
        val NoOfPartitions = (jobxmlNode \ "@noOfPartitions").text.trim
        val LowerBound = (jobxmlNode \ "@lowerBound").text.trim
        val UpperBound = (jobxmlNode \ "@upperBound").text.trim
        val ForceNoPersist = (jobxmlNode \ "@forceNoPersist").text.trim

        val JDBCSql = (jobxmlNode \ "sql").text.trim
        if ("" != JDBCPropsFile) {
            JobProps.putAll(pub.ayada.scala.utils.hdfs.HdfsFileIO.getProps(JDBCPropsFile))
        }

        new pub.ayada.scala.sparkUtils.etl.read.jdbc.Jdbc2DFProps(id = JDBCId,
            propsFile = JDBCPropsFile,
            schema = UtilFuncs.getPropOrElse(JobProps, "schema", JDBCSchema),
            table = UtilFuncs.getPropOrElse(JobProps, "table", JDBCTable),
            fetchSize = UtilFuncs.getPropOrElse(JobProps, "fetchSize", JDBCFetchSize),
            isolationLevel = UtilFuncs.getPropOrElse(JobProps, "isolationLevel", JDBCIsolationLevel),
            repartition = UtilFuncs.getPropOrElse(JobProps, "repartition", { if (Repartition.isEmpty) "0" else Repartition }).toInt,
            loadCount = ("true" == UtilFuncs.getPropOrElse(JobProps, "loadCount", JDBCLoadCount)),
            printSchema = ("true" == UtilFuncs.getPropOrElse(JobProps, "printSchema", JDBCPrintSchema)),
            broadcast = ("true" == UtilFuncs.getPropOrElse(JobProps, "Broadcast", Broadcast)),
            sql = {
                if ("" != JDBCSql)
                    JDBCSql
                else {
                    logs.append(DateTimeUtils.getFmtDtm("Driver-Jdbc2DF"))
                        .append("Custom SQL not provided. Using the input schema and table to build the sql.\n")
                    if ("" != JDBCSchema) {
                        "select * from " + JDBCSchema + "." + JDBCTable
                    } else {
                        "select * from " + JDBCTable
                    }
                }
            },
            partitionKey = UtilFuncs.getPropOrElse(JobProps, "partitionKey", PartitionKey),
            noOfPartitions = UtilFuncs.getPropOrElse(JobProps, "noOfPartitions", NoOfPartitions),
            lowerBound = UtilFuncs.getPropOrElse(JobProps, "lowerBound", LowerBound),
            upperBound = UtilFuncs.getPropOrElse(JobProps, "upperBound", UpperBound),
            forceNoPersist = { "true" == UtilFuncs.getPropOrElse(JobProps, "forceNoPersist", ForceNoPersist) },
            connParms = scala.collection.mutable.Map("url" -> UtilFuncs.getPropOrFail(JobProps, "JDBCUrl"),
                "driver" -> UtilFuncs.getPropOrFail(JobProps, "JDBCDriver"),
                "user" -> UtilFuncs.getPropOrFail(JobProps, "JDBCUser"),
                "password" -> UtilFuncs.getPropOrElse(JobProps, "JDBCPassword", UtilFuncs.getPswdFromAlias(JobProps, sc)))
        )
    }
}

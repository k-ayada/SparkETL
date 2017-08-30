package pub.ayada.scala.sparkUtils.etl.read.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.storage.StorageLevel

import pub.ayada.scala.utils.DateTimeUtils

import pub.ayada.scala.sparkUtils.cmn.UtilFuncs
import pub.ayada.scala.sparkUtils.etl.read.ReadProps
import pub.ayada.scala.sparkUtils.etl.read.Common
import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs

class Hive2DF(jobID : String,
              hiveContext : HiveContext,
              defaults : scala.collection.mutable.Map[String, String],
              props : pub.ayada.scala.sparkUtils.etl.read.hive.Hive2DFProps,
              dfs : scala.collection.mutable.Map[String, TransformerDFs]) {
    val taskID = "Hive2DF-" + jobID + "-" + props.id
    def execute() = {
        import org.apache.spark.sql._
        import hiveContext.implicits._

        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(taskID) + "Building DataFrame")
        try {

            val (df, msg) = Common.getTransformerDF(taskID, defaults, props, hiveContext.sql(props.sql))
            df.df.registerTempTable(props.id)
            dfs.put(props.id, df)
            println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(taskID) + msg)
            if (props.printSchema) {
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(taskID) + "DF schema of : " + props.id + "\n" + df.df.schema.treeString)
            }
        } catch {
            case t : Exception =>
                println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(taskID) + "Failed to process Hive read task.")
                t.printStackTrace()
                throw new Exception(t)
        }
    }

}

object Hive2DF {
    /**
      * extracts the Hive src from the input xml node object. If a property is
      * defined in both propsFile and in the xml, the value in the xml will take precedence
      * <br>List of properties, <code>
      * <br>hive \"@propsFile" &nbsp;&nbsp; (default:null)
      * <br>hive \"@schema" &nbsp;&nbsp; (default:null)
      * <br>hive \"@table" &nbsp;&nbsp; (default:null)
      * <br>hive \"@fetchSize" &nbsp;&nbsp; (default: "100000")
      * <br>hive \"@isolationLevel" &nbsp;&nbsp; (default:"READ_COMMIT")
      * <br>hive \"@loadCount" &nbsp;&nbsp; (default: "false")
      * <br>hive \"@printSchema" &nbsp;&nbsp; (default: "false")
      * <br>hive \"@repartition" &nbsp;&nbsp; (default: "false")
      * <br>hive \"@broadcast" &nbsp;&nbsp; (default: "false")
      * <br>hive \"sql" &nbsp;&nbsp; (default:null)
      * <code>
      */
    def getHive2DFProp(jobxmlNode : scala.xml.Node,
                       defaults : scala.collection.mutable.Map[String, String],
                       logs : StringBuilder) : pub.ayada.scala.sparkUtils.etl.read.hive.Hive2DFProps = {

        var JobProps : java.util.Properties = new java.util.Properties
        val PropsFile = (jobxmlNode \ "@propsFile").text.trim
        val Id = (jobxmlNode \ "@id").text.trim
        val Schema = (jobxmlNode \ "@schema").text.trim
        val Table = (jobxmlNode \ "@table").text.trim
        val LoadCount = (jobxmlNode \ "@loadCount").text.trim.toLowerCase
        val PrintSchema = (jobxmlNode \ "@printSchema").text.trim.toLowerCase
        val Repartition = (jobxmlNode \ "@repartition").text.trim
        val Broadcast = (jobxmlNode \ "@broadcast").text.trim.toLowerCase
        val ForceNoPersist = (jobxmlNode \ "@forceNoPersist").text.trim

        val Sql = (jobxmlNode \ "sql").text.trim

        new pub.ayada.scala.sparkUtils.etl.read.hive.Hive2DFProps(id = Id,
            schema = UtilFuncs.getPropOrElse(JobProps, "schema", Schema),
            table = UtilFuncs.getPropOrElse(JobProps, "table", Table),
            loadCount = ("true" == UtilFuncs.getPropOrElse(JobProps, "loadCount", LoadCount)),
            printSchema = ("true" == UtilFuncs.getPropOrElse(JobProps, "printSchema", PrintSchema)),
            repartition = UtilFuncs.getPropOrElse(JobProps, "repartition", { if (Repartition.isEmpty) "0" else Repartition }).toInt,
            broadcast = ("true" == UtilFuncs.getPropOrElse(JobProps, "Broadcast", Broadcast)),
            forceNoPersist = { "true" == UtilFuncs.getPropOrElse(JobProps, "forceNoPersist", ForceNoPersist) },
            sql = {
                if ("" != Sql)
                    Sql
                else {
                    logs.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Hive2DF"))
                        .append("Custom SQL not provided. Using the input schema and table to build the sql.\n")
                    if ("" != Schema) {
                        "select * from " + Schema + "." + Table
                    } else {
                        "select * from " + Table
                    }
                }
            }
        )
    }

}

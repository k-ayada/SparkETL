package pub.ayada.scala.sparkUtils.etl.read.csv

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.storage.StorageLevel

import pub.ayada.scala.utils.DateTimeUtils

import pub.ayada.scala.sparkUtils.cmn.UtilFuncs
import pub.ayada.scala.sparkUtils.etl.read.ReadProps
import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs
import pub.ayada.scala.sparkUtils.etl.read.Common

class Csv2DF(jobID : String,
             hiveContext : HiveContext,
             defaults : scala.collection.mutable.Map[String, String],
             props : pub.ayada.scala.sparkUtils.etl.read.csv.Csv2DFProps,
             dfs : scala.collection.mutable.Map[String, TransformerDFs]) {

    private val taskID = "Csv2DF-" + jobID + "-" + props.id
    //private val hiveContext = hiveCtx
    import hiveContext.sql
    import hiveContext.implicits._
    import org.apache.spark.sql._

    val dfOptions : scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String](
        "header" -> props.header.toString,
        "delimiter" -> props.delimiter,
        "quote" -> props.quote,
        "escape" -> props.escape,
        "mode" -> props.mode,
        "charset" -> props.charset,
        "comment" -> { if ("" == props.commentChar) null else props.commentChar.charAt(0).toString },
        "dateFormat" -> props.dateFormat,
        "nullValue" -> props.nullValue)

    def execute() = {
        println(DateTimeUtils.getFmtDtm(taskID) + "All Props:" + pub.ayada.scala.utils.ObjectUtils.prettyPrint(props))
        println(DateTimeUtils.getFmtDtm(taskID) + "DataFrame options: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(dfOptions))
        println(DateTimeUtils.getFmtDtm(taskID) + "Schema: " + pub.ayada.scala.utils.ObjectUtils.prettyPrint(props.csvColumns.simpleString))

        val dfr = if (props.csvColumns.isEmpty) {
            dfOptions += ("inferSchema" -> "true")
            hiveContext.read
                .format("com.databricks.spark.csv")
                .options(dfOptions)
        } else {
            hiveContext.read
                .format("com.databricks.spark.csv")
                .options(dfOptions)
                .schema(props.csvColumns)
        }
        try {
            val (df, msg) = Common.getTransformerDF(taskID, defaults, props, dfr.load(props.path))
            dfs.put(props.id, df)
            df.df.registerTempTable(props.id)
            println(DateTimeUtils.getFmtDtm(taskID) + msg)
            if (props.printSchema) {
                println(DateTimeUtils.getFmtDtm(taskID) + "DF schema of : " + props.id + "\n" + df.df.schema.treeString)
            }
        } catch {
            case t : Exception =>
                println(DateTimeUtils.getFmtDtm(taskID) + "Failed to process Hive read task.")
                t.printStackTrace()
                throw new Exception(t)
        }
    }
}

object Csv2DF {
    /**
      * extracts the CSV jobinfo from the input xml node object. If a property is
      * defined in both propsFile and in the xml, the value in the xml will take precedence
      * <br><br>List of properties, <code>
      * <br>"csv" \ "@propsFile" &nbsp;&nbsp; (default:null)
      * <br>"csv" \ "@path" &nbsp;&nbsp; (default:null)
      * <br>"csv" \ "@header" &nbsp;&nbsp; (default:false)
      * <br>"csv" \ "@delimiter" &nbsp;&nbsp; (default:",")
      * <br>"csv" \ "@quote" &nbsp;&nbsp; (default:'"')
      * <br>"csv" \ "@escape" &nbsp;&nbsp; (default:"\")
      * <br>"csv" \ "@mode" &nbsp;&nbsp; (default:PERMISSIVE)
      * <br>"csv" \ "@charset" &nbsp;&nbsp; (default:UTF-8)
      * <br>"csv" \ "@commentChar" &nbsp;&nbsp; (default:null)
      * Default is "#". Disable comments by setting this to null.
      * <br>"csv" \ "@dateFormat" &nbsp;&nbsp; (default: null)
      * supports formats defined in java.text.SimpleDateFormat. if null,
      * java.sql.Timestamp.valueOf() and java.sql.Date.valueOf() will be used to
      * convert the string
      * <br>"csv" \ "@nullValue" &nbsp;&nbsp; (default:null)
      * <br>"csv" \ "@loadCount" &nbsp;&nbsp; (default:null)
      * <br>"csv" \ "filterSql" &nbsp;&nbsp; (default:null) If
      * not mentioned, all the valid rows will be processed.
      * <br>"csv" \ "schema" &nbsp;&nbsp; (default:null) If
      * not provided, schema will be inferred.
      * <br>"csv" \ "schema" \  "@printSchema" &nbsp;&nbsp; (default:null)
      * <code>
      */

    def getCsv2DFProp(jobxmlNode : scala.xml.Node,
                      defaults : scala.collection.mutable.Map[String, String],
                      logs : StringBuilder) : pub.ayada.scala.sparkUtils.etl.read.csv.Csv2DFProps = {
        var JobProps : java.util.Properties = new java.util.Properties()
        val PropsFile = (jobxmlNode \ "@propsFile").text.trim
        val Path = (jobxmlNode \ "@path").text.trim
        val Header = (jobxmlNode \ "@header").text.trim
        val Delimiter = (jobxmlNode \ "@delimiter").text.trim
        val Quote = (jobxmlNode \ "@quote").text.trim
        val Escape = (jobxmlNode \ "@escape").text.trim
        val Mode = (jobxmlNode \ "@mode").text.trim.toUpperCase
        val Charset = (jobxmlNode \ "@charset").text.trim
        val CommentChar = (jobxmlNode \ "@commentChar").text.trim
        val DateFormat = (jobxmlNode \ "@dateFormat").text.trim
        val NullValue = (jobxmlNode \ "@nullValue").text.trim
        val LoadCount = (jobxmlNode \ "@loadCount").text.trim.toLowerCase
        val PrintSchema = (jobxmlNode \ "schema" \ "@printSchema").text.trim.toLowerCase
        val Repartition = (jobxmlNode \ "@repartition").text.trim
        val Broadcast = (jobxmlNode \ "@broadcast").text.trim.toLowerCase
        val ForceNoPersist = (jobxmlNode \ "@forceNoPersist").text.trim

        val CSVColumns = StructType(for (col <- jobxmlNode \ "schema" \ "column") yield {
            StructField((col \ "@name").text.trim,
                pub.ayada.scala.sparkUtils.cmn.UtilFuncs.literal2SparkDataType((col \ "@dataType").text.trim),
                "false" != (col \ "@nullable").text.trim.toLowerCase
            )
        })
        if ("" != PropsFile) {
            JobProps.putAll(pub.ayada.scala.utils.hdfs.HdfsFileIO.getProps(PropsFile))
        }
        new pub.ayada.scala.sparkUtils.etl.read.csv.Csv2DFProps(
            id = (jobxmlNode \ "@id").text.trim,
            propsFile = PropsFile,
            path = UtilFuncs.getPropOrElse(JobProps, "propsFile", Path),
            delimiter = UtilFuncs.getPropOrElse(JobProps, "delimiter", if ("" != Delimiter) Delimiter else ","),
            quote = UtilFuncs.getPropOrElse(JobProps, "quote", if ("" != Quote) Quote else "\""),
            escape = UtilFuncs.getPropOrElse(JobProps, "escape", if ("" != Escape) Escape else "\\"),
            mode = UtilFuncs.getPropOrElse(JobProps, "mode", if ("" != Mode) Mode else "PERMISSIVE"),
            charset = UtilFuncs.getPropOrElse(JobProps, "charset", if ("" != Charset) Charset else "UTF-8"),
            commentChar = UtilFuncs.getPropOrElse(JobProps, "commentChar", if ("" != CommentChar) String.valueOf(CommentChar.charAt(0)) else ""),
            dateFormat = UtilFuncs.getPropOrElse(JobProps, "dateFormat", if ("" != DateFormat) DateFormat else ""),
            nullValue = UtilFuncs.getPropOrElse(JobProps, "nullValue", if ("" != NullValue) NullValue else ""),
            header = { "true" == UtilFuncs.getPropOrElse(JobProps, "header", Header) },
            printSchema = { "true" == UtilFuncs.getPropOrElse(JobProps, "printSchema", PrintSchema) },
            loadCount = { "true" == UtilFuncs.getPropOrElse(JobProps, "loadCount", LoadCount) },
            repartition = UtilFuncs.getPropOrElse(JobProps, "repartition", { if (Repartition.isEmpty) "0" else Repartition }).toInt,
            broadcast = { "true" == UtilFuncs.getPropOrElse(JobProps, "broadcast", Broadcast) },
            forceNoPersist = { "true" == UtilFuncs.getPropOrElse(JobProps, "forceNoPersist", ForceNoPersist) },
            csvColumns = CSVColumns)
    }
}

package pub.ayada.scala.sparkUtils.etl.write.parquet

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import pub.ayada.scala.utils.DateTimeUtils
import pub.ayada.scala.sparkUtils.cmn.UtilFuncs
import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs

object DF2Parquet {

    def write2Parquet(jobID : String,
                      hiveContext : HiveContext,
                      dF2ParquetProps : DF2ParquetProps,
                      df : TransformerDFs) : Unit = {

        val taskID = "DF2Hive-" + jobID + "-" + dF2ParquetProps.srcDF

        println(DateTimeUtils.getFmtDtm(taskID) + "Load properties: " + dF2ParquetProps.toString())

        //Drive the write mode
        var mode = {
            if ("overwrite" == dF2ParquetProps.loadType || (!isFileAvailable(taskID, hiveContext, dF2ParquetProps))) {
                org.apache.spark.sql.SaveMode.Overwrite
            } else {
                //report the pre-load load count if requested.
                if (dF2ParquetProps.preLoadCount) {
                    rptRecordCount(taskID, " before load: ", hiveContext, dF2ParquetProps)
                }
                org.apache.spark.sql.SaveMode.Append
            }
        }

        var d : DataFrame = {
            if (df.persist != null && df.persist != org.apache.spark.storage.StorageLevel.NONE) {
                println(DateTimeUtils.getFmtDtm(taskID) + "Persisting with Storagelevel : " + df.persist.description)
                df.loadCount = false
                df.df.persist(df.persist)
            } else {
                df.df
            }
        }

        if (df.loadCount) {
            print(DateTimeUtils.getFmtDtm(taskID) + "Number of records to be inserted from the DF " + dF2ParquetProps.srcDF + ": ")
            println(df.df.count)
        }

        println(DateTimeUtils.getFmtDtm(taskID) + "Writing the DF: " + dF2ParquetProps.srcDF + " to the Parquet file : " + dF2ParquetProps.fileDir + " mode: " + mode)
        if (dF2ParquetProps.partitionColumns.length > 0) {
            df.df.write.partitionBy(dF2ParquetProps.partitionColumns : _*).mode(mode).parquet(dF2ParquetProps.fileDir)
        }
        try { df.df.unpersist() }
        catch { case t : Throwable => }

        if (dF2ParquetProps.postLoadCount) {
            rptRecordCount(taskID, " after load: ", hiveContext, dF2ParquetProps)
        }

    }

    /**
      * Extracts the jobinfo related to hive load from the input xml node object. <br>
      * If a property is defined in both propsFile and in the xml, the value in the xml will take precedence
      * <br><br>List of properties, <code>
      * <br>"parquet" \ "@id"
      * <br>"parquet" \ "@srcDF"
      * <br>"parquet" \ "@loadType" &nbsp;&nbsp; (default:append)
      * <br>"parquet" \ "@fileDir"
      * <br>"parquet" \ "@partitionColumns" &nbsp;&nbsp; (default:null)
      * <br>"parquet" \ "@preLoadCount" &nbsp;&nbsp; (default:false)
      * <br>"parquet" \ "@postLoadCount" &nbsp;&nbsp; (default:false)
      * <code>
      */
    def getDF2ParquetProps(jobxmlNode : scala.xml.Node, logs : StringBuilder) : DF2ParquetProps = {

        val FileDir = (jobxmlNode \ "@fileDir").text.trim

        if ("" != FileDir) {
            val Id = (jobxmlNode \ "@id").text.trim
            val SrcDF = (jobxmlNode \ "@srcDF").text.trim
            val LoadType = (jobxmlNode \ "@loadType").text.trim.toLowerCase

            val PreLoadCount = ("true" == (jobxmlNode \ "@preLoadCount").text.trim.toLowerCase)
            val PostLoadCount = ("true" == (jobxmlNode \ "@postLoadCount").text.trim.toLowerCase)
            val PartitionColumns = (jobxmlNode \ "@partitionColumns").text.trim.toLowerCase.split(",")

            new DF2ParquetProps(id = Id,
                srcDF = SrcDF,
                loadType = LoadType,
                fileDir = FileDir,
                preLoadCount = PreLoadCount,
                postLoadCount = PostLoadCount,
                partitionColumns = PartitionColumns)

        } else {
            logs.append(DateTimeUtils.getFmtDtm("Driver-DF2Parquet"))
                .append("Err: Parquet File path is blank. Ignoring the parquet load:\n")
                .append(new scala.xml.PrettyPrinter(250, 4).format(jobxmlNode))

            null
        }

    }

    def isFileAvailable(jobPropsId : String, hiveContext : HiveContext, props : DF2ParquetProps) : Boolean = {
        val res : Boolean = UtilFuncs.isFileExists(props.fileDir, hiveContext.sparkContext)
        if (props.loadType == "append" && !res)
            props.loadType = "overwrite"
        res
    }

    def rptRecordCount(jobPropsId : String, step : String, hiveContext : HiveContext, props : DF2ParquetProps) = {
        val count = hiveContext.read.parquet(props.fileDir).count()
        println(DateTimeUtils.getFmtDtm(jobPropsId) +
            "Number of records in " + props.fileDir + step + count)
    }

}

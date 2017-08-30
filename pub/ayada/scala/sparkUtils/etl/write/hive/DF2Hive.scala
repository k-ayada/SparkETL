package pub.ayada.scala.sparkUtils.etl.write.hive
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrameWriter
import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs
import pub.ayada.scala.utils.DateTimeUtils

object DF2Hive {

    def write2Hive(jobID : String,
                   hiveContext : HiveContext,
                   dF2HiveProps : pub.ayada.scala.sparkUtils.etl.write.hive.DF2HiveProps,
                   df : TransformerDFs) : Unit = {
        val schemaTable = dF2HiveProps.schema + "." + dF2HiveProps.table
        val taskID = "DF2Hive-" + jobID + "-" + dF2HiveProps.srcDF

        println(DateTimeUtils.getFmtDtm(taskID) + "Load properties: " + dF2HiveProps.toString())

        var mode = if ("overwrite" == dF2HiveProps.loadType || (!isTableAvailable(taskID, hiveContext, dF2HiveProps))) {
            org.apache.spark.sql.SaveMode.Overwrite
        } else {
            if (dF2HiveProps.preLoadCount) {
                rptHiveRecordCount(taskID, " before load: ", hiveContext, dF2HiveProps)
            }
            org.apache.spark.sql.SaveMode.Append
        }

        var d : org.apache.spark.sql.DataFrame = {
            if (df.persist != null && df.persist != StorageLevel.NONE) {
                val xD = df.df.persist(df.persist)
                print(DateTimeUtils.getFmtDtm(taskID) +
                    "Persisting with Storagelevel : " + df.persist.toString +
                    "Number of records to be inserted from the DF " + dF2HiveProps.srcDF + ": ")
                println(xD.count)
                df.loadCount = false
                xD
            } else {
                df.df
            }
        }
        if (df.loadCount) {
            print(DateTimeUtils.getFmtDtm(taskID) +
                "Number of records to be inserted from the DF " +
                dF2HiveProps.srcDF + ": ")
            println(d.count)
        }

        println(DateTimeUtils.getFmtDtm(taskID) +
            "Writing the DF: " + dF2HiveProps.srcDF + " to the hive table: " + schemaTable + " mode: " + mode + ", format: " + dF2HiveProps.format)

        /*
        var dfw : DataFrameWriter = d.write

        if (dF2HiveProps.partitionColumns.length > 0) {

            hiveContext.setConf("hive.exec.dynamic.partition", "true")
            hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

            dfw = dfw.partitionBy(dF2HiveProps.partitionColumns : _*)
        }
*/
        if (mode == org.apache.spark.sql.SaveMode.Overwrite) {
            d.write.mode(mode).format(dF2HiveProps.format).saveAsTable(schemaTable)

        } else {
            d.write.mode(mode).format(dF2HiveProps.format).insertInto(schemaTable)
        }

        try { d.unpersist() }
        catch { case t : Throwable => }

        hiveContext.refreshTable(schemaTable)

        if (dF2HiveProps.postLoadCount) {
            rptHiveRecordCount(taskID, " after load: ", hiveContext, dF2HiveProps)
        }

    }
    /**
      * Extracts the jobinfo related to hive load from the input xml node object. <br>
      * If a property is defined in both propsFile and in the xml, the value in the xml will take precedence
      * <br><br>List of properties, <code>
      * <br>"hive" \ "@loadType"
      * <br>"hive" \ "@schema"
      * <br>"hive" \ "@table"
      * <br>"hive" \ "@preLoadCount" &nbsp;&nbsp; (default:false)
      * <br>"hive" \ "@postLoadCount" &nbsp;&nbsp; (default:false)
      * <code>
      */
    def getDF2HiveProps(jobxmlNode : scala.xml.Node, logs : StringBuilder) : pub.ayada.scala.sparkUtils.etl.write.hive.DF2HiveProps = {
        val Table = (jobxmlNode \ "@table").text.trim.toUpperCase

        if ("" != Table) {
            val LoadId = (jobxmlNode \ "@id").text.trim
            val SrcDF = (jobxmlNode \ "@srcDF").text.trim
            val LoadType = (jobxmlNode \ "@loadType").text.trim.toLowerCase
            val Schema = (jobxmlNode \ "@schema").text.trim.toUpperCase
            val PreLoadCount = ("true" == (jobxmlNode \ "@preLoadCount").text.trim.toLowerCase)
            val PostLoadCount = ("true" == (jobxmlNode \ "@postLoadCount").text.trim.toLowerCase)
            val Format = (jobxmlNode \ "@format").text.trim.toLowerCase
            val PartitionColumns = (jobxmlNode \ "@partitionColumns").text.trim.toLowerCase.split(",")

            new pub.ayada.scala.sparkUtils.etl.write.hive.DF2HiveProps(id = LoadId,
                srcDF = SrcDF,
                loadType = LoadType,
                schema = { if ("" == Schema) "DEFAULT" else Schema },
                table = Table,
                format = { if ("" == Format) "parquet" else Format },
                partitionColumns = PartitionColumns,
                preLoadCount = PreLoadCount,
                postLoadCount = PostLoadCount)

        } else {
            logs.append(DateTimeUtils.getFmtDtm("Driver-DF2Hive"))
                .append("Err: Table name is blank. Ignoring the hive load:\n")
                .append(new scala.xml.PrettyPrinter(250, 4).format(jobxmlNode))

            null
        }

    }

    def isTableAvailable(jobPropsId : String, hiveContext : HiveContext, dF2HiveProps : pub.ayada.scala.sparkUtils.etl.write.hive.DF2HiveProps) : Boolean = {

        val sql = "show tables in " + dF2HiveProps.schema
        println(DateTimeUtils.getFmtDtm(jobPropsId) + "Checking tables using sql : " + sql)
        val tblDF1 = hiveContext.sql(sql).filter("isTemporary = false")

        val tblDF = tblDF1.map(t => if (t(1).asInstanceOf[Boolean] == false) t(0).asInstanceOf[String]).collect().filter(_ != null) // t(0) - table name , t(1) - is temp table
        println(DateTimeUtils.getFmtDtm(jobPropsId) + "Number of non-Temp tables found: " + tblDF.size + " List: " + tblDF.mkString(","))
        val tbls = for (tbl <- tblDF) yield {
            if (tbl != null) String.valueOf(tbl).toUpperCase
        }

        val schemaTable = new StringBuilder(dF2HiveProps.schema).append(".").append(dF2HiveProps.table).toString

        if (tbls.contains(dF2HiveProps.table)) {
            //https://spark.apache.org/docs/latest/sql-programming-guide.html#metadata-refreshing
            hiveContext.refreshTable(dF2HiveProps.schema + "." + dF2HiveProps.table)
            return true
        }

        println(DateTimeUtils.getFmtDtm(jobPropsId) + "Table '" + schemaTable + "' not found. Forcing the hive load mode to Overwrite.")
        dF2HiveProps.loadType = "overwrite"
        false
    }

    def rptHiveRecordCount(jobPropsId : String, step : String, hiveContext : HiveContext, dF2HiveProps : pub.ayada.scala.sparkUtils.etl.write.hive.DF2HiveProps) = {
        val schemaTable = new StringBuilder(dF2HiveProps.schema).append(".").append(dF2HiveProps.table).toString
        val countSQL = "select count(*) as cnt from " + schemaTable
        val x = hiveContext.sql(countSQL)
            .map(t => t(0))
            .collect()(0)
        println(DateTimeUtils.getFmtDtm(jobPropsId) +
            "Number of records in " + schemaTable + step + x)
    }
}

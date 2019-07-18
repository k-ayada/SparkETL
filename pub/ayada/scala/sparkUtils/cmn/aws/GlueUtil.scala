package  
import java.net.URI
import scala.collection.mutable.Buffer
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import com.gwf.datalake.util.SparkUtil
import scala.collection.JavaConverters._

import com.amazonaws.auth.InstanceProfileCredentialsProvider

class GlueUtil(val spark: SparkSession) {

  def getDBPath(db: String): String = {
    val dbPathsAsRow = spark.sql(s"describe database ${db}")
      .filter("database_description_item = 'Location'")
      .select("database_description_value")
      .collect()

    if (dbPathsAsRow.length > 0) {
      return dbPathsAsRow(0).getString(0)
    }
    null
  }

  def getExtTblPath(db: String, tbl: String): String = {
    if (spark.catalog.tableExists(db, tbl)) {
      val loc = spark.sql(s"describe formatted ${db}.${tbl}")
        .filter("col_name = 'Location'")
        .select("data_type")
        .take(1)
        .map(locRow => locRow.getString(0))
      loc(0)
    } else {
      null
    }
  }  
  
  def getTableFormat(db: String, tbl: String):String = {
    if (spark.catalog.tableExists(s"${db}.${tbl}")){
    val inpFrmt = spark.sql(s"describe formatted ${db}.${tbl}")
      .filter("col_name = 'InputFormat'")
      .select("data_type")
      .take(1)
      .map(locRow => locRow.getString(0))   
    
       if (inpFrmt.contains("parquet"))
           return "parquet"
       else if (inpFrmt.contains("orc"))
            return"orc"
       else if (inpFrmt.contains("Text"))    
            return"csv"         
    }
    null
    
  }

  def getTables(db: String, tblPtrn: String): Array[String] = {
    spark.sql(s"show tables in ${db}")
      .filter("isTemporary = false ")
      .filter(s"tableName like '${tblPtrn}'")
      .select("tableName")
      .collect()
      .map(tbsRow => tbsRow.getString(0))
  }

  def deletePath(hdpCfg: org.apache.hadoop.conf.Configuration, path: String, recursive: Boolean, printSummary: Boolean): Boolean = {

    val splits: Array[String] = path.split("/")
    val fs = FileSystem.get(new URI(s"${splits(0)}//${splits(2)}"), hdpCfg)

    val pth = new org.apache.hadoop.fs.Path(path)

    try {
      if (fs.exists(pth)) {        
        SparkUtil.log(s"GlueUtil.deletePath(): Path found. sent delete request. Path: ${pth}")
        SparkUtil.log(s"GlueUtil.deletePath(): Summary: \n\t ${fs.getContentSummary(pth)}")
        fs.delete(pth, recursive)
        true
      } else {
        SparkUtil.log(s"GlueUtil.deletePath(): Path not found. So, it is not deleted. Path: ${pth}")
        false
      }
    } catch {
      case e: Throwable => {
        SparkUtil.log(s"GlueUtil.deletePath(): Failed to purge the path. So, it is not deleted. Path: ${pth}")
        e.printStackTrace()
        return false
      }
    }
  }

  def dropTable(db: String, dbPath: String, tbl: String) = {

    try {
      SparkUtil.log(s"GlueUtil.dropTable(): Removing  table '${db}.${tbl}' under the path: ${dbPath}/${tbl}")
      deletePath(spark.sparkContext.hadoopConfiguration, s"${dbPath}/${tbl}", true, true)
      spark.sql(s"drop table if exists ${db}.${tbl}")

    } catch {
      case t: Throwable =>
        SparkUtil.log(s"GlueUtil.dropTable(): Failed to drop work table: '${db}.${tbl}'. Please handle it manually")
        t.printStackTrace()
    }
  }
  
}
object GlueUtil {
  
  def getTablesInfo (db:String):Buffer[com.amazonaws.services.glue.model.Table] = {
    val glue = com.amazonaws.services.glue.AWSGlueClientBuilder
                   .standard()
                   .withCredentials(InstanceProfileCredentialsProvider.getInstance())
                   .build
        val gtreq = new com.amazonaws.services.glue.model.GetTablesRequest
        gtreq.setDatabaseName(db)
        glue.getTables(gtreq).getTableList.asScala
  }
  
  def getTableList(db:String):Array[String] = { 
        val gtres = getTablesInfo(db).map(t => { t.getName })
        gtres.toArray
  }
  
  def getTableLocation(db:String, table:String) : String = { 
    val locs = getTablesInfo(db).filter(t => t.getName == table).map( t => t.getStorageDescriptor.getLocation)
    locs(0)
  }
  
  
  def main(argArr: Array[String]): Unit = {
    val argMap = com.gwf.datalake.util.SparkUtil.args2map(argArr)
    val spark = SparkSession
      .builder
      .appName("GlueUtils")
      .enableHiveSupport()
      .getOrCreate()

    val utl = new GlueUtil(spark)

    val db = argMap.get("--db").get
    val dbPath = utl.getDBPath( db)
    if (argMap.contains("--tblList")) {
      val list = argMap.get("--tblList").get.split(",")
      for (tbl <- list) {
        utl.dropTable(db, dbPath, tbl)
      }
    }

    if (argMap.contains("--tblPtrn")) {
      val list = utl.getTables( db, argMap.get("--tblPtrn").get)
      for (tbl <- list) {
        utl.dropTable(db, dbPath, tbl)
      }
    }
  }
}

/*

spark-submit --deploy-mode client --class com.gwf.datalake.util.aws.GlueUtil s3://prod-gwf-datalake-artifact-us-east-1/Jars/Marketing-0.1-jar-with-dependencies.jar
--db marketing 

--tblList plansdf --tblPtrn "mrkt%"
*/

package pub.ayada.scala.sparkUtils.cmn

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap,Buffer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Column, Row, Dataset, SparkSession,DataFrameWriter }

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.Coalesce

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.model.ObjectMetadata

import com.gwf.datalake.util.aws.GlueUtil
import com.gwf.datalake.util.hdfs.HDFSUtil
import com.gwf.datalake.util.aws.S3Util


class SparkUtil(implicit private[this] val sysArgs: scala.collection.mutable.Map[String, String], val spark: SparkSession) {

  private val s3Util = new S3Util()
  private val glueUtil = new GlueUtil(spark)
  private val hdfsUtil = new HDFSUtil(spark.sparkContext.hadoopConfiguration)
  private val runMode = sysArgs.get("--runMode").get
  private val explainDF = sysArgs.contains("-explainDF")
  private val saveDF = sysArgs.contains("-saveDF") || sysArgs.contains("--saveDFAs")
  private val saveDFAs = sysArgs.getOrElse("--saveDFAs", "NONE")
  private val printCnt = sysArgs.contains("-printCounts")
  private val printDFCnt = sysArgs.contains("-printDFCnt")
  private val oldAppId = sysArgs.getOrElse("--oldAppId", null)
  private val workDB = db("work")
  private val useHint = sysArgs.contains("-useHint")
  private val useHive4Work = sysArgs.contains("-useHive4Work")
  private val useS3ForHive = sysArgs.contains("-useS3ForHive")
  
  private val hdfsTempDfStore = s"${kv("--tempHDFS")}/${workDB}"  

  private val dbPaths = scala.collection.mutable.Map[String, String]()
  private val dfltRDDPartitions = {    
     val inst = spark.conf.get("spark.executor.instances", "20").toInt
     val core = spark.conf.get("spark.executor.cores", "4").toInt    
    inst * core * 2
  }

  private val worktbAs: String = if (useHive4Work || saveDFAs == "HIVE" ) {
    SparkUtil.log(s"SparkUtil.dbPath(): Using Hive DB ${workDB} to store the intermediate tables.")
    spark.sql(s"use $workDB")
    if (useS3ForHive) {
      SparkUtil.log(s"SparkUtil.dbPath(): Using s3 as storage for the work DB ${workDB}. Path ${kv("--tempS3")}")
      dbPaths.put(workDB.toLowerCase, kv("--tempS3"))
      kv("--tempS3")     
    } else {
      SparkUtil.log(s"SparkUtil.dbPath(): Using HDFS as storage for the work DB ${workDB}. Path ${hdfsTempDfStore}")
      dbPaths.put(workDB.toLowerCase, hdfsTempDfStore)
      hdfsTempDfStore      
    }
    "HIVE"
  } else {
    SparkUtil.log(s"SparkUtil.dbPath(): Using HDFS files to store the intermediate tables. Path ${hdfsTempDfStore}")
    "HDFS"
  }
  
  def workTblAs : String = worktbAs
  
  def delHDFSDir(dfName:String) : Unit = {
    hdfsUtil.delDirs(s"{hdfsTempDfStore}/${dfName}")
  }

  def getSparkSession: SparkSession = {
    spark
  }

  def db(key: String): String = {
    key.toLowerCase match {
      case "rep_cmnm"  => kv("--db-REP_CMNM", "prod_rep")
      case "easy"      => kv("--db-EASY", "easy")
      case "dw"        => kv("--db-DW", "prod_dw")
      case "dm"        => kv("--db-DM", "prod_dm")
      case "etl_rapid" => kv("--db-ETLR", "etl_rapid")
      case "etl_dm"    => kv("--db-ETLDM", "etl_dm")
      case "sfmc"      => kv("--db-SFMC", "sfmc")
      case "cirrus"    => kv("--db-CIRRUS", "cirrus")
      case "static"    => kv("--db-STATIC", "static_db")
      case "mrkt"      => kv("--db-MRKT", "marketing")
      case "work"      => kv("--db-WORK", "work")
      case _           => throw new Exception(s"Unknown Database key '$key'. Valid values are 'easy','dw','dm','etlr','edm', 'sfmc', 'cirrus'")
    }
  }

  def dbPath(db: String): String = {
    val d = db.toLowerCase
    if (dbPaths.contains(d)) {
      dbPaths.get(d).get
    } else {     
      val pth = if ((!d.startsWith("work")) || kv("--hiveTblsAs", "ext") == "mgd") {
        glueUtil.getDBPath(d) //its not a work DB or we need work DBs managed path. Get the path form GLue
      } else  {
        if (useS3ForHive) kv("--tempS3")  else  hdfsTempDfStore
      }
      dbPaths.put(d, pth)
      pth
    }

  }

  def kv(key: String, default: String = null): String = {
    sysArgs.getOrElse(key, default);
  }

  /**
   * Return the formatted system date. Format : <code>yyyyMMdd hh:mm:ss</code>
   *  @parm none
   */
  private def formattedDate(): String = {
    SparkUtil.dtFmt.format(new java.util.Date())
  }

  /**
   * Executes the input SQL against Hive/DataFrame Temp view and returns the result Dataframe.
   *  This functions also created the Spark temp view provided the resDFName is not null
   *
   *  @param dfName: Name of the Spark temp view to be created
   *  @param resDFName name of the temp table for the result Dataset
   *  @param persistTyeStr: String representation of PersistType
   *  @param sql: SQL to be executed
   *  @param partitions: number of partitions to use .
   *  @return SQL result as Dataset
   */
  def sql(resDFName: String, persistTyeStr: String, query: String, partitions: Int = 0): Dataset[Row] = {
    SparkUtil.log(s"\n\nSparkUtil.sql(): Creating ${resDFName} , persistTyeStr: ${persistTyeStr} ")
    val df = storeDF(sql(query, partitions), resDFName, persistTyeStr, partitions, SparkUtil.getPartitionColumnsFromSQL(query))
    if (resDFName != null) {
      df.createOrReplaceTempView(resDFName)
    }
    
    if (printCnt) {
      SparkUtil.log(s"SparkUtil.sql(): Number of records loaded to '${if (resDFName != null) resDFName else "noDFName"}' : ${df.count}")
    }
    df
  }
  /**
   * Executes the input SQL against Hive/DataFrame Temp view and returns the result Dataframe.
   *  This functions does not created the Spark temp view.
   *
   *  @param dfName: Name of the Spark temp view to be created
   *  @param sql: SQL to be executed
   *  @param partitions: number of partitions to use . If 0, don't repartition
   *  @return SQL result as Dataset
   */
  def sql(sql: String, partitions: Int): Dataset[Row] = {
    val sqlStr = handleHints(sql)
    SparkUtil.log(s"SparkUtil.sql(): UseHint: ${useHint} ${sqlStr}")
    val df = if (partitions > 0) {
      val df = spark.sql(sqlStr)
      if (df.rdd.getNumPartitions < partitions) {
        df.repartition(partitions)
      } else {
        df.coalesce(partitions)
      }
    } else {
      spark.sql(sqlStr)
    }
     if (printDFCnt){
        SparkUtil.log(s"SparkUtil.sql(): Number of records in DF : ${df.count}")   
     }
    df 
  }

  /**
   * runs spark.catalog.dropTempView(view)
   */
  def dropTempView(view: String): Unit = {
    spark.catalog.dropTempView(view)
  }

  /**
   * runs spark.sparkContext.broadcast(df)
   */
  def broadcastDF(df: Dataset[Row]): Unit = {
    spark.sparkContext.broadcast(df)

  }

  /**
   * Reads the parquet file and returns the result Dataframe.
   *
   */
  def readParquet(filePath: String, structure: StructType = null, dfName: String = null, mergeSchema: Boolean = false, filter: String = ""): Dataset[Row] = {
    SparkUtil.log(s"SparkUtil.readParquet(): Reading ${filePath}")
    val rdr = spark.read.option("mergeSchema", mergeSchema.toString).format("parquet")
    if (structure != null) rdr.schema(structure)
    val df = rdr.load(filePath)
    if (dfName != null) {
      df.createOrReplaceTempView(dfName)
    }
    df
  }
  /**
   * Reads the csv file and returns the result Dataframe.
   *
   */
  def readCsv(filePath: String, options:HashMap[String,String] = null,structure: StructType = null, dfName: String = null): Dataset[Row] = {
    SparkUtil.log(s"SparkUtil.readCsv(): Reading ${filePath}")
    val rdr = spark.read.format("csv")
    if (structure != null) {
      SparkUtil.log(s"SparkUtil.readCsv():  Forcing the structue: ${structure.toString}")
      rdr.schema(structure)
    }
    if (options != null) {
      SparkUtil.log(s"SparkUtil.readCsv():  Read options: ${options.toString}")
      rdr.options(options)
    }
    val df = rdr.load(filePath)
    if (dfName != null) {
      df.createOrReplaceTempView(dfName)
    }
    df
  }
  
  /**
   * Reads the text file and returns the result Dataframe.
   *
   */
  def readTextFile(filePath: String, dfName: String = null, persistTyeStr:String): Dataset[Row] = {
    SparkUtil.log(s"SparkUtil.readTextFile(): Reading ${filePath}")    
    val df = storeDF(spark.read.format("text").load(filePath), dfName, persistTyeStr) 
    if (dfName != null) {
      df.createOrReplaceTempView(dfName)
    }
    df
  }
  
  /**
   * Reads the ORC file and returns the result Dataframe.
   *
   */
  def readOrc(filePath: String, structure: StructType = null, dfName: String = null, mergeSchema: Boolean = false): Dataset[Row] = {
    SparkUtil.log(s"SparkUtilreadOrc(): Reading ${filePath}")
    val rdr = spark.read.format("org.apache.spark.sql.execution.datasources.orc")
    if (structure != null) rdr.schema(structure)
    val df = rdr.load(filePath)
    if (dfName != null) {
      df.createOrReplaceTempView(dfName)
    }
    df
  }

  /**
   */
  def storeDF(dataFrame: Dataset[Row], table: String, persistTypStr: String, numPartitions: Int = 0, partitionCols: Array[String] = null): Dataset[Row] = {

    if (explainDF && (saveDF || "NULL|NONE".indexOf(persistTypStr.toUpperCase) < 0)) {
      dataFrame.explain
      SparkUtil.log("\n\n\n")
    }

    val saveType = if (saveDF && persistTypStr.toUpperCase != "HIVE" && persistTypStr.toUpperCase != "NULL") saveDFAs else persistTypStr.toUpperCase
    val curParts = dataFrame.rdd.getNumPartitions
    
    val df = {
      if (saveType == "HDFS" || saveType == "S3" || saveType == "HIVE") {
        val cnt = Math.min(kv("--outFileCnt").toInt, curParts)
        if (numPartitions == -1) {
          if (runMode == "t") SparkUtil.log(s"storeDF(): saveType: ${saveType} #of cur partions: ${curParts}, New ${dfltRDDPartitions}")  
          dataFrame.repartition(dfltRDDPartitions)
        }
        else if (numPartitions > 1) {
          if (runMode == "t") SparkUtil.log(s"storeDF(): saveType: ${saveType} #of cur partions: ${curParts}, New ${numPartitions}")  
          dataFrame.repartition(numPartitions)
        }
        else if (cnt == numPartitions || numPartitions == 0 ) {
          if (runMode == "t") SparkUtil.log(s"storeDF(): saveType: ${saveType} #of cur partions: ${curParts}. Not done any repartition")
          dataFrame
        } else {
          if (runMode == "t") SparkUtil.log(s"storeDF(): saveType: ${saveType} #of cur partions: ${curParts}, New ${cnt}")  
          dataFrame.repartition(cnt)
        }
      } else {
        dataFrame
      }
    }

    val df2 = saveType match {
      case "NULL" => df
      case "NONE" => df
      case "HDFS" => persistExternal(s"${hdfsTempDfStore}", table, df, partitionCols)
      case "S3"   => persistExternal(kv("--tempS3"), table, df, partitionCols)
      case "HIVE" => persistToHive(table, df, partitionCols)
      case "CHECK_POINT" => df.cache.checkpoint(true)     
      case _ => {
        persistLocal(df, table, SparkUtil.getPersistType(persistTypStr))
        if (!printCnt) {
          val row = df.take(1) // force persist
        }  
        df
      }
    }    
    df2

  }

  def waitForS3File(path: String): Boolean = {
    if (runMode == "t")
      SparkUtil.log(s"SparkUtil.waitForS3File(): '${path}/_SUCCESS'")
    var cnt = 0
    var found = false
    while (found == false && cnt < kv("--s3WaitIter", "20").toInt) {
      Thread.sleep(kv("--rwInterval").toLong)
      if (s3Util.isKeyInS3(s"${path}/_SUCCESS")) {
        found = true
      } else {
        cnt += 1
        SparkUtil.log(s"SparkUtil.waitForS3File(): Waiting for file '${path}/_SUCCESS' ? : ${cnt} iterations.")
      }
    }
    SparkUtil.log(s"SparkUtil.waitForS3File(): Found '${path}/_SUCCESS' ? ${found} after ${cnt} iterations.")
    if (sysArgs.contains("-printFiles")) {
      SparkUtil.log(s"SparkUtil.waitForS3File(): List of files,")
      for (fl <- s3Util.listObjectsRecursive(path).asScala) {
        SparkUtil.log(s"SparkUtil.waitForS3File():\t ${fl}")
      }
    }

    return found
  }

  def persistExternal(path: String, file: String, df: Dataset[Row], partitionCols: Array[String] = null, overwrite: Boolean = true): Dataset[Row] = {
    val fileFormat = kv("--fileFormat","parquet").toLowerCase    
    val pth = s"${path}/${file}"
    val st = df.schema
    write2ExtrFile(fileFormat,path, file, df, partitionCols, overwrite)
    if (path.startsWith("s3")) waitForS3File(path)      
 
    fileFormat match {
      case "parquet" => readParquet(s"${pth}", st)
      case "csv" => readCsv(s"${pth}",null, st)
      case "orc" => readOrc(s"${pth}", st)
      case _ => readParquet(s"${pth}", st)
    }
    
    
    val df1 = readParquet(s"${pth}", st)
    df.unpersist()
    df1
  }
  def write2ExtrFile(fileFormat:String ,path: String, file: String, df: Dataset[Row], partitionCols: Array[String] = null, overwrite: Boolean = true): Unit = {
    val pth = s"${path}/${file}"
    val wrtr:DataFrameWriter[Row] = if (partitionCols != null) {
      SparkUtil.log(s" persistExternal:- Persisting DataFrme to ${pth} with partitions: ${partitionCols.mkString(",")} . ")

      df.write
        .mode(if (overwrite) org.apache.spark.sql.SaveMode.Overwrite else org.apache.spark.sql.SaveMode.Append)
        .partitionBy(partitionCols: _*)
        
    } else {
      SparkUtil.log(s" persistExternal:- Persisting DataFrme to ${pth} with partitions: none")
      df.write
        .mode(if (overwrite) org.apache.spark.sql.SaveMode.Overwrite else org.apache.spark.sql.SaveMode.Append)        
    }   
    
    fileFormat.toLowerCase match {
      case "parquet" => wrtr.parquet(s"${pth}")
      case "csv" => wrtr.csv(s"${pth}")
      case "orc" => wrtr.orc(s"${pth}")
      case _ => wrtr.parquet(s"${pth}")
    }
    
  }
  

  private def persistToHive(file: String, df: Dataset[Row], partitionCols: Array[String] = null): Dataset[Row] = {
    df.createOrReplaceTempView(s"${file}_df")
     if (kv("--hiveTblsAs","mgd") == "mgd"){
       save2Hive(workDB, file, s"${file}_df", partitionCols)
     } else {
       save2HiveExt(workDB, file, s"${file}_df", partitionCols)  
     }   
    
    df.unpersist
    spark.catalog.dropTempView(s"${file}_df")
    val tbPath = s"${dbPath(workDB)}/${file}"
    if (tbPath.startsWith("s3")) waitForS3File(tbPath)
    
    spark.sql(s"REFRESH TABLE ${workDB}.${file}")
    
    val sqlStr = if (partitionCols != null && partitionCols.length > 0) {
      s"select * from ${workDB}.${file} " //distribute by ${partitionCols.mkString(",")} "
    } else {
      s"select * from ${workDB}.${file}"
    }
    val df1 = sql(sqlStr, 0)
    df1
  }

  def save2HiveExt(db: String, table: String, dfName: String = "null", partitionCols: Array[String] = null, overwrite: Boolean = true): Unit = {
    val df = if (dfName == "null") table else dfName

    val ddlStr = new StringBuilder()

    val partitions = if (partitionCols != null && partitionCols.length > 0) {
      s" PARTITIONED BY ( ${partitionCols.mkString(",")} ) "
    } else {
      ""
    }

    //if overwrite is false, we are assuming that table exists and just need to append records to the parquet file.
    if (overwrite) {
      val Tblpath = glueUtil.getExtTblPath(db, table)

      spark.sql(s"drop table if exists ${db}.${table}")   
      if (Tblpath == null) {
        if (runMode == "t") SparkUtil.log(s"save2HiveExt():- Table $db.$table not found. So, not deleted.")
      } else if (Tblpath.startsWith("s3")) {
        s3Util.deletePath(spark.sparkContext.hadoopConfiguration, Tblpath, true, (runMode == "t"))
      } else {
        hdfsUtil.delDirs(Tblpath)
      }
      
      val pth = s"${dbPath(db)}/${table}"
      ddlStr.append(s"create table ${db}.${table} using ${kv("--fileFormat", "parquet")} ")
      ddlStr.append(partitions)
      ddlStr.append(s"  LOCATION '${pth}'")
      ddlStr.append(s"  as select * from ${df}")
    } else {
      ddlStr.append(s"insert into ${db}.${table} ${partitions} select * from ${df}")
    }
    SparkUtil.log(s"SparkUtil.save2HiveExt(): Running SQL : ${ddlStr.toString}")
    spark.sql(ddlStr.toString)
  }
  
  def save2Hive(db: String, table: String, dfName: String = "null", partitionCols: Array[String] = null, overwrite: Boolean = true): Unit = {
    val df = if (dfName == "null") table else dfName
    val partitions = if (partitionCols != null && partitionCols.length > 0) {
      s"PARTITIONED BY ( ${partitionCols.mkString(",")} )"
    } else {
      ""
    }

    val sql = if (overwrite) {
      val dbPth = dbPath(db)
      spark.sql(s"drop table if exists ${db}.${table}")
      
      if (dbPth == null) {
        if (runMode == "t") SparkUtil.log(s"SparkUtil.save2Hive():- DB Path not found for DB: ${db}. Please update it in Glue.")
      } else if (dbPth.startsWith("s3")) {
        s3Util.deletePath(spark.sparkContext.hadoopConfiguration, s"${dbPth}/${table}", true, (runMode == "t"))
      } else {
        hdfsUtil.delDirs(s"${dbPth}/${table}")
      }
      
      s"create table ${db}.${table} using ${kv("--fileFormat","parquet")} ${partitions} as select * from ${df}"
    } else {
      s"insert into ${db}.${table} ${partitions} select * from ${df}"
    }
 

    if (runMode == "t") {
      SparkUtil.log(s"SparkUtil.save2Hive(): Running SQL : $sql")
    }
    spark.sql(sql)
  }

  private def persistLocal(df: Dataset[Row], table: String, storageLevel: StorageLevel): Unit = {
    spark.sharedState.cacheManager.cacheQuery(df, Some(table), storageLevel)
    if (runMode == "t") {
      SparkUtil.log("SparkUtil.persistLocal() : List of RDDs persisted.")
      for (r <- spark.sparkContext.getRDDStorageInfo) {
        SparkUtil.log(s"${r.toString()}")
      }
    }
  }

  /**
   * Based on the flag, retain or remove hints from SQL.
   *  @param SQL string
   *  @return SQL string
   */
  private def handleHints(sql: String): String = {
    if (useHint)
      sql
    else {
      sql.replaceAll("/\\*\\+.*\\*/", "")
    }
  }

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

  def clearWorkTables() {
    try {
      val tbls = glueUtil.getTables(workDB,"mrkt%")
      SparkUtil.log(s"clearWorkTables(): Removing work tables that startsWith 'mrkt%' from ${workDB}")
      for (tbl <- tbls) {
        try {
          val tblPath = glueUtil.getExtTblPath(workDB,tbl)
          SparkUtil.log(s"clearWorkTables(): removing ${workDB}.${tbl} @ path: ${tblPath} ")
          deletePath(tblPath, true, runMode == "t")
          spark.sql(s"drop table if exists ${workDB}.${tbl}")
        } catch {
          case t: Exception => {
            SparkUtil.log(s"SparkUtil.clearWorkTables(): Failed to drop work table: '${workDB}.${tbl}'. Please handle it manually")
            SparkUtil.log(s"SparkUtil.clearWorkTables(): ${ScalaUtil.getStackTraceAsStr(t)}") 
          }
        }
      }
    } catch {
      case t: Exception => {
        SparkUtil.log(s"SparkUtil.clearWorkTables(): Failed to drop work tables in the Database : ${workDB}")
        SparkUtil.log(s"SparkUtil.clearWorkTables(): ${ScalaUtil.getStackTraceAsStr(t)}") 
      }
    }
  }

  def deletePath(path: String, recursive: Boolean, printSummary: Boolean): Boolean = {

    val fs = if (path.startsWith("hdfs") || path.charAt(0) == '/') {
      org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    } else {
      val splits: Array[String] = path.split("/")
      org.apache.hadoop.fs.FileSystem.get(new URI(s"${splits(0)}//${splits(2)}"), spark.sparkContext.hadoopConfiguration)
    }

    val pth = new org.apache.hadoop.fs.Path(path)
    try {
      if (fs.exists(pth)) {
        
        SparkUtil.log(s"deletePath(): Path found. sent delete request. Path: ${pth}")
        if (printSummary) {
          SparkUtil.log(s"deletePath(): Summary: \n\t ${fs.getContentSummary(pth)}")
        }
        fs.delete(pth, recursive)
        true
      } else {
        SparkUtil.log(s"deletePath(): Path not found. So, it is not deleted. Path: ${pth}")
        false
      }
    } catch {
      case e: Throwable => {
        SparkUtil.log(s"SparkUtil.deletePath(): Failed to Delete path ${pth}")
        SparkUtil.log(s"SparkUtil.deletePath(): ${ScalaUtil.getStackTraceAsStr(e)}") 
        return false
      }
    }
  }
  
  def getBucket(uri:String):String = {    
    s3Util.asS3URI(uri).getBucket
  }
  
  def getKey(uri:String):String = {    
    s3Util.asS3URI(uri).getKey
  }
  
  def copyS3Object(src_bucketName: String, sourceKey: String, dst_bucketName: String, destinationKey: String){
     if (runMode == "t") {
       SparkUtil.log(s"copyS3Object():- srcBkt: ${src_bucketName}, srcKey:${sourceKey}")
       SparkUtil.log(s"copyS3Object():- dstBkt: ${dst_bucketName}, dstKey:${destinationKey}")
     }
    s3Util.copyObject(src_bucketName, sourceKey, dst_bucketName, destinationKey)
  }
  def copyS3Object(src_bucketName: String, sourceKey: String, srcPrtn:String, dst_bucketName: String, destinationKey: String){
     if (runMode == "t") {
       SparkUtil.log(s"copyS3Object():- srcBkt: ${src_bucketName}, srcKey:${sourceKey}")
       SparkUtil.log(s"copyS3Object():- dstBkt: ${dst_bucketName}, dstKey:${destinationKey}")
     }
    s3Util.copyObject(src_bucketName, sourceKey, srcPrtn,dst_bucketName, destinationKey)
  }

  def delHDFSFile(path: String): Unit = {
    if (hdfsUtil.fileExists(path)){    
      SparkUtil.log(s"Clearing the HDFS Path: ${path}")
      hdfsUtil.listSubFolders(path)
      hdfsUtil.delDirs(path)
    }  else {
      SparkUtil.log(s"Path not found. Not deleting it: ${path}")
    }
  }
  
  def listS3Objects(bucketName: String, key: String, pattern: String): Buffer[(String, String, String)] = {    
    val objSmry = s3Util.listObjects(bucketName, key, pattern)
    objSmry.map(o => {
      val fullPath = o.getKey
      val pthArr = o.getKey.split("/")
      val file = pthArr(pthArr.length - 1)
      val fldr = pthArr.dropRight(1).mkString("/")
      (o.getBucketName, fldr, file)
    })
    
  }
  def getS3ObjectMetadata(bucket:String, key:String):ObjectMetadata  = {
      s3Util.getObjectMetadata(bucket,key)
  }
}

object SparkUtil {
  private val startTime = System.currentTimeMillis();
  private val dtFmt = new java.text.SimpleDateFormat("yyyyMMdd hh:mm:ss")

  def getSparkConf(argsMap: scala.collection.mutable.Map[String, String]): SparkConf = {

    val hm = scala.collection.mutable.Map[String, String]()
    hm.put("spark.rps.askTimeout", "1200")
    hm.put("spark.network.timeout", "1200")
    hm.put("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
    hm.put("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    hm.put("spark.kryo.unsafe","false")
    hm.put("spark.kryoserializer.buffer","10240")
    hm.put("spark.kryoserializer.buffer.max","2040m")
    hm.put("mapreduce.fileoutputcommitter.algorithm.version" ,"2")
    hm.put("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")

    
/*  hm.put("spark.sql.tungsten.enabled" ,"true")
    hm.put("spark.sql.cbo.enabled","true")
    hm.put("spark.sql.cbo.joinReorder.enabled","true")
    hm.put("park.sql.session.timeZone","MDT")
   
    hm.put("spark.sql.constraintPropagation.enabled","true")
    hm.put("spark.sql.parquet.filterPushdown","true")
    hm.put("spark.sql.hive.metastorePartitionPruning" ,"true")
    hm.put("spark.hadoop.parquet.enable.summary-metadata", "false")
*/
  
    if (argsMap.contains("-supressBroadcast") || !argsMap.contains("-useHint")) {
      hm.put("spark.sql.autoBroadcastJoinThreshold", "-1")
    }    
    else {      
      hm.put("spark.broadcast.blockSize", "16m")
      hm.put("spark.sql.broadcastTimeout", "1200")
      hm.put("spark.broadcast.compress", "true")      
      hm.put("spark.rdd.compress", "true")
    }
    
    
    { new SparkConf() }.setAll(hm)
  }

  def getSparkSession(argsMap: scala.collection.mutable.Map[String, String]): SparkSession = {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    System.setProperty("com.amazonaws.services.s3.enforceV4", "true")
    
    val sc = getSparkConf(argsMap)
    val spark = SparkSession
      .builder
      .appName(argsMap.getOrElse("--AppName", this.getClass.getCanonicalName))
      .config(sc)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel(argsMap.getOrElse("--logLevel", "WARN"))

    spark.conf.set("fs.s3.enableServerSideEncryption", "true")
    //spark.conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
    //spark.conf.set("fs.s3a.server-side-encryption.key", argsMap.get("--KMSKey").get)
    //spark.conf.set("fs.s3.serverSideEncryption.kms.keyId", argsMap.get("--KMSKey").get)

    val hdpCnf = spark.sparkContext.hadoopConfiguration
    //hdpCnf.set("fs.s3.enableServerSideEncryption", "true")
    //hdpCnf.set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
    //hdpCnf.set("fs.s3a.server-side-encryption.key", argsMap.get("--KMSKey").get)

    //hdpCnf.set("fs.s3.serverSideEncryption.kms.keyId", argsMap.get("--KMSKey").get)

    //hdpCnf.set("fs.s3a.fast.upload", "true")
    //hdpCnf.set("dfs.blocksize", "134217728")
    //hdpCnf.set("parquet.block.size", "134217728")
    //hdpCnf.set("fs.s3a.block.size", "134217728")
    hdpCnf.set("io.file.buffer.size", "65536") //https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-troubleshoot-errors-io.html
     
    hdpCnf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") //TODO use parameter store
    hdpCnf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    if (argsMap.contains("--s3Acc") && argsMap.contains("--s3Sec")) {
      hdpCnf.set("fs.s3a.access.key", argsMap.getOrElse("--s3Acc", null))
      hdpCnf.set("fs.s3a.secret.key", argsMap.getOrElse("--s3Sec", null))
    } else {//if (argsMap.contains("-AWSProfileAuth")) {
      log(s" Using InstanceProfileCredentialsProvider Authentication")
      val cre: AWSCredentials =  com.amazonaws.auth.InstanceProfileCredentialsProvider.getInstance().getCredentials
      hdpCnf.set("fs.s3a.access.key", cre.getAWSAccessKeyId)
      hdpCnf.set("fs.s3a.secret.key", cre.getAWSSecretKey)
    //SparkUtil.log(s"<${cre.getAWSAccessKeyId}><${cre.getAWSSecretKey}>")
    }

    //SparkUtil.log(s" Spark Confs:\n\t ${spark.sparkContext.getConf.getAll.mkString("\n\t")}\n\n")
    SparkUtil.log(s"SparkUtil.getSparkSession(): Spark Confs,\n${spark.sparkContext.getConf.toDebugString}\n\n")
    
    
    //printlns(s" Hadoop Confs:\n\t ${hdpCnf.toString}\n\n")
    import spark.implicits._
    spark
  }

  /**
   * Converts the input Array of String arguments to convert to key value pairs
   *
   *  use '--' for arguments key value pairs. ex: --runType PROD
   *  </br>
   *  use '-' for single value argument. ex: -verbose
   */
  def args2map(argsArr: Array[String]): scala.collection.mutable.Map[String, String] = {

    var i: Int = 0
    val map = scala.collection.mutable.Map[String, String]()

    while (i <= argsArr.size - 1) {
      if ("--" == argsArr(i).substring(0, 2)) {
        map.put(argsArr(i), argsArr(i + 1))
        i = i + 1
      } else {
        map.put(argsArr(i), argsArr(i))
      }
      i = i + 1
    }

    log(s" Args:\n\t${map.mkString("\n\t")}\n\n")
    map
  }

  /**
   * Return the formatted system date. Format : yyyyMMdd hh:mm:ss
   */
  private def formattedDate(): String = {
    dtFmt.format(new java.util.Date())
  }

  def log(string: String) = {
    val elapsed = "%3d".format(((System.currentTimeMillis() - startTime) / 1000) / 60)
    val dt = formattedDate
    string.split("\n").foreach { s =>
      System.out.println(s"${dt} ${elapsed} ${s}")
      System.err.println(s"${dt} ${elapsed} ${s}")
    }
  }

  /**
   * Queries the Hive MetaData catalog and retrieves the location of the Database
   */
  def getDBLoc(db: String)(implicit spark: SparkSession): String = {

    val dbs = spark.sql("show databases")
    log(s" Databases accessible:\n")
    dbs.collect.foreach { x => SparkUtil.log(s"\t${x.get(0)}"); }

    if (dbs.filter(s"databaseName = '${db}'").collect().length > 0) {
      val desc = spark.sql(s"describe database ${db}")

      try {
        log(s"Details of the work DB (if present): '${db}'")
        desc.collect().map { row => log(row.toString) }
        val locArr = desc.filter("database_description_item = 'Location'").collect()
        val loc = if (locArr.length > 0) locArr(0).getString(1) else null
        SparkUtil.log(s" Location of ${db}:  ${loc}")
        loc
      } catch {
        case t: Throwable =>
          null
      }

    } else {
      null
    }

  }
  /**
   * Returns the Spark application ID from the Spark Conf
   */
  def getSparkAppId()(implicit spark: SparkSession, utl: SparkUtil, argMap: scala.collection.mutable.Map[String, String]) = {

    if (argMap.contains("--oldAppId"))
      argMap.get("--oldAppId").get
    else if (spark.conf.getAll.contains("spark.yarn.app.id"))
      spark.conf.get("spark.yarn.app.id") // EMR cluster mode or non EMR Cluster
    else
      spark.conf.get("spark.app.id") //EMR client mode
  }

  /**
   * Creates the the directory needed. Function needs full path including the protocol. Example hdfs://|s3a://|file://
   */
  def createFile(path: String, delIfExists: Boolean = true, deleteOnExit: Boolean = false)(implicit spark: SparkSession): Boolean = {
    try {
      val fs = if (path.startsWith("hdfs") || path.charAt(0) == '/') {
        org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      } else {
        val splits: Array[String] = path.split("/")
        org.apache.hadoop.fs.FileSystem.get(new URI(s"${splits(0)}//${splits(2)}"), spark.sparkContext.hadoopConfiguration)
      }

      val pth = new org.apache.hadoop.fs.Path(path)

      if (fs.exists(pth)) {
        if (delIfExists)
          fs.delete(pth, true)
      } else {
        fs.create(pth)

        if (deleteOnExit)
          fs.deleteOnExit(pth)
      }
      true
    } catch {
      case e: Throwable => {
        log(s"Failed to create the file : $path")
        log(s"SparkUtil.createFile(): ${ScalaUtil.getStackTraceAsStr(e)}")
        return false
      }
    }
  }
  /**
   * Purges the path passed. Function needs full path including the protocol. Example hdfs://|s3a://|file://
   */
  def deletePath(path: String, recursive: Boolean, printSummary: Boolean)(implicit spark: SparkSession): Boolean = {
    val fs = if (path.startsWith("hdfs") || path.charAt(0) == '/') {
      org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    } else {
      val splits: Array[String] = path.split("/")
      org.apache.hadoop.fs.FileSystem.get(new URI(s"${splits(0)}//${splits(2)}"), spark.sparkContext.hadoopConfiguration)
    }

    val pth = new org.apache.hadoop.fs.Path(path)
    try {
      if (fs.exists(pth)) {
         log(s" Path found. sent delete request. Path: ${pth}")
         if (printSummary) {
           log(s" Summary: ${fs.getContentSummary(pth)}")
        }
        fs.delete(pth, recursive) 
        true
      } else {
        log(s" Path not found. So, it is not deleted. Path: ${pth}")
        false
      }
    } catch {
      case e: Throwable => {
        log(s"SparkUtil.deletePath(): Failed to purge the path: ${pth}")
        SparkUtil.log(s"SparkUtil.deletePath(): ${ScalaUtil.getStackTraceAsStr(e)}") 
        return false
      }
    }
  }

  /**
   * converts String to the StorageLevel type
   */
  def getPersistType(persistTypStr: String): StorageLevel = persistTypStr.toUpperCase match {
    case "NONE"                  => StorageLevel.NONE
    case "DISK_ONLY"             => StorageLevel.DISK_ONLY
    case "DISK_ONLY_2"           => StorageLevel.DISK_ONLY_2
    case "MEMORY_ONLY"           => StorageLevel.MEMORY_ONLY
    case "MEMORY_ONLY_2"         => StorageLevel.MEMORY_ONLY_2
    case "MEMORY_ONLY_SER"       => StorageLevel.MEMORY_ONLY_SER
    case "MEMORY_ONLY_SER_2"     => StorageLevel.MEMORY_ONLY_SER_2
    case "MEMORY_AND_DISK"       => StorageLevel.MEMORY_AND_DISK
    case "MEMORY_AND_DISK_2"     => StorageLevel.MEMORY_AND_DISK_2
    case "MEMORY_AND_DISK_SER"   => StorageLevel.MEMORY_AND_DISK_SER
    case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
    case "OFF_HEAP"              => StorageLevel.OFF_HEAP
    case _ => {
      SparkUtil.log(s" Invalid Storage value sting '${persistTypStr}' receieved. Setting to StorageLevel.NONE")
      StorageLevel.NONE
    }
  }

  def handleSpecialChar(char: String, ln: String): String = {
    val inx = ln.indexOf(char)
    val out = if (inx < ln.length() - 1) {
      new StringBuilder()
        .append(ln.substring(0, inx))
        .append(char)
        .append(ln.substring(inx + 1).capitalize)
        .toString
    } else { ln }
    //println(s"handleSpecialChar : in $ln & out: $out")
    out
  }

  def handleHyphenAndSingleQuote(ln: String): String = {
    val out = if (ln.indexOf("-") > 0) {
      handleSpecialChar("-", if (ln.indexOf("'") < 0) ln else handleSpecialChar("'", ln))
    } else if (ln.indexOf("'") > 0) {
      handleSpecialChar("'", ln)
    } else {
      ln
    }
    //println(s"handleHyphenAndSingleQuote : in $ln & out: $out")
    out
  }

  def format_last_name(name: String): String = {
    val ln = name.trim.toUpperCase
    try {
      val splits = name.replaceAll(" +", " ").split(" ")
      val sb = new StringBuilder()
      var i = 0
      while (i < splits.length - 1) {
        sb.append(splits(i).toLowerCase).append(" ")
        i += 1
      }
      if (ln.endsWith(" IX") ||
        ln.endsWith(" X") ||
        ln.endsWith(" IV") ||
        ln.endsWith(" VIII") ||
        ln.endsWith(" VII") ||
        ln.endsWith(" VI") ||
        ln.endsWith(" V") ||
        ln.endsWith(" III") ||
        ln.endsWith(" II")) {
        sb.append(splits(splits.length - 1)).toString
      } else { sb.append(splits(splits.length - 1).toLowerCase).toString }

      handleHyphenAndSingleQuote(sb.toString)
        .split(" ").map { x => x.capitalize }.mkString(" ")

    } catch {
      case t: Throwable => {
        SparkUtil.log(s"SparkUtil.format_last_name(): Faild to parse name: $name ")
        SparkUtil.log(s"SparkUtil.format_last_name(): ${ScalaUtil.getStackTraceAsStr(t)}")
        null
      }
    }
  }
  def decTrunc(decVal: java.math.BigDecimal, pre: java.lang.Integer): java.math.BigDecimal = {
    if (decVal == null)
      return decVal
    val x = decVal.setScale(pre, java.math.BigDecimal.ROUND_DOWN)
    x
  }

  def getPartitionColumnsFromSQL(sql: String): Array[String] = {
    val s = sql.toLowerCase.trim

    val inx = s.indexOf(" cluster ")

    if (inx > 0) {
      s.substring(inx + 12).split(",").map(_.trim)
    } else {
      val frm = s.indexOf(" distribute ")

      if (frm > 0) {
        val to = s.indexOf(" sort ", frm + 15)
        if (to > frm) {
          s.substring(frm + 15, to).split(",").map(_.trim)
        } else {
          s.substring(frm + 15).split(",").map(_.trim)
        }
      } else {
        null
      }
    }
  }

  def toSparkSqlColumns(cols: Array[String]): Array[Column] = {

    if (cols == null) {
      null
    } else {
      cols.map { c => new Column(c) }

    }
  }

  //CORE-26293 - new UDF for spark
  def frm_zoned(p_val: String, p_dec: Int = 0): String = {
    if (p_dec > p_val.length) {
      throw new RuntimeException(s"SparkUtil.frm_zoned(): Invalid arguments. Length of decimals ($p_dec) > total filed length (${p_val.length})")
    }

    val l_ascii = if (p_dec == 0) {
      p_val
    } else {
      val int_parrt_len = p_val.length - p_dec
      s"${p_val.substring(0, int_parrt_len)}.${p_val.substring(int_parrt_len)}"
    }
    val l_SignChar = l_ascii.substring(l_ascii.length() - 1, l_ascii.length)
    val l_asc = l_SignChar.toUpperCase match {
      case "{" => l_ascii.replace("{", "0") case "}" => s"-${l_ascii.replace("}", "0")}"
      case "A" => l_ascii.replace("A", "1") case "J" => s"-${l_ascii.replace("J", "1")}"
      case "B" => l_ascii.replace("B", "2") case "K" => s"-${l_ascii.replace("K", "2")}"
      case "C" => l_ascii.replace("C", "3") case "L" => s"-${l_ascii.replace("L", "3")}"
      case "D" => l_ascii.replace("D", "4") case "M" => s"-${l_ascii.replace("M", "4")}"
      case "E" => l_ascii.replace("E", "5") case "N" => s"-${l_ascii.replace("N", "5")}"
      case "F" => l_ascii.replace("F", "6") case "O" => s"-${l_ascii.replace("O", "6")}"
      case "G" => l_ascii.replace("G", "7") case "P" => s"-${l_ascii.replace("P", "7")}"
      case "H" => l_ascii.replace("H", "8") case "Q" => s"-${l_ascii.replace("Q", "8")}"
      case "I" => l_ascii.replace("I", "9") case "R" => s"-${l_ascii.replace("R", "9")}"
      case _ => throw new RuntimeException(s"SparkUtil.frm_zoned(): Unknown character $l_SignChar in the laste byte of the zoned decimal filed")
    }
    l_asc
  }
}

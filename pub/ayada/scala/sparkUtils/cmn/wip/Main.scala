package com.gwf.datalake.marketing

import com.gwf.datalake.util.SparkUtil
import org.apache.spark.sql.SparkSession
import java.io.StringWriter
import java.io.PrintWriter
import com.gwf.datalake.util.aws.SSMUtil
import com.gwf.datalake.marketing.plans.PlansToCsv
import com.gwf.datalake.marketing.parts.PartsToCsv

object Main {

  def forceLogLevel(level: String = "WARN") =  {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.toLevel(level))
  }

  def getDataFromParameterStore(): scala.collection.mutable.Map[String, String] = {
    val map = scala.collection.mutable.Map[String, String]()
    val ssm = new SSMUtil
    map.put("--datalakeBucket", ssm.getParameter("/datalake/s3/etl/bucketName", true))
    map.put("--tempS3", ssm.getParameter("/datalake/s3/tempFolder", true))
    map.put("--db-REP_CMNM", ssm.getParameter("/datalake/databases/cmnm", true))
    map.put("--db-EASY", ssm.getParameter("/datalake/databases/easy", true))
    map.put("--db-DW", ssm.getParameter("/datalake/databases/surf/prod_dw/dbname", true))
    map.put("--db-DM", ssm.getParameter("/datalake/databases/mrta/prod_dm/dbname", true))
    map.put("--db-ETLR", ssm.getParameter("/datalake/databases/surf/etl_rapid/dbname", true))
    map.put("--db-ETLDM", ssm.getParameter("/datalake/databases/mrta/etl_dm/dbname", true))
    map.put("--db-SFMC", ssm.getParameter("/datalake/sfdc/sfmc/dbname", true))
    map.put("--db-CIRRUS", ssm.getParameter("/datalake/sfdc/cirrus/dbname", true))
    map.put("--db-STATIC", ssm.getParameter("/datalake/databases/static", true))
    map.put("--db-WORK", ssm.getParameter("/datalake/databases/work", true))
    map
  }

  def handleArgs(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    
//  val map = getDataFromParameterStore()  //Get the defaults from  the parameter store    
    val map = scala.collection.mutable.Map[String, String]()
    
    val mapx = SparkUtil.args2map(args) // Get the overrides from commandline
    
    for ((k,v) <- mapx) map.put(k,v)  //override the defaults

    if (!map.contains("--dateFormat")) {
      map.put("--dateFormat", "yyyyMMdd")
    }
    if (!map.contains("--runMode") ) {
      map.put("--runMode", "p")
    }
    if (!map.contains("--runEle")) {
        map.put("--runEle", "all")
    }
    if (!map.contains("--rwInterval")) {
        map.put("--rwInterval", "2000")
    }
    
    if (!map.contains("--source_system")) {
        map.put("--source_system", "")
    } else {
        map.put("--source_system", s"source_system= '${map.get("--source_system").get}' ")
    }
    
    if (!map.contains("--fileFormat")) {
        map.put("--fileFormat", "PARQUET")
    }
    
    if (!map.contains("--hiveTblsAs")) {
        map.put("--hiveTblsAs", "ext")
    }
    
    if (!map.contains("--sgwBucket")) {
        map.put("--sgwBucket", "s3://prod-gwf-datalake-storage-gateway-us-east-1")
    }
 
    if ((!map.contains("--buildCSV")) ) {
        if (map.get("--runEle").get == "all")
          map.put("--buildCSV", "plans,parts")
        else 
          map.put("--buildCSV", "no") 
    }
    
    if ((!map.contains("-doRepartition")) ) {
        map.put("-doRepartition", "no")
    }
    
    if ((!map.contains("--s3WaitIter")) ) {
        map.put("--s3WaitIter", "20")
    }
    
    if ((!map.contains("--outFileCnt")) ) {
        map.put("--outFileCnt", "5")
    }
   
    map.put("--tempHDFS", "hdfs:///temp")
    forceLogLevel(map.getOrElse("--logLevel", "WARN"))
    
    map
  }
  def createWorkArea()(implicit spark: SparkSession, utl: SparkUtil, argMap: scala.collection.mutable.Map[String, String]): Unit = {
    utl.delHDFSFile(utl.kv("--tempHDFS"))
    SparkUtil.createFile(s"${utl.kv("--tempHDFS")}/.ignore", true, true)
    spark.sparkContext.setCheckpointDir(s"${utl.kv("--tempHDFS")}/checkpoint");

    if (!argMap.contains("--tempS3")) {
      //utl.deletePath(s"${utl.kv("--datalakeBucket")}/temp/mrkt/", true, true)
      argMap.put("--tempS3", s"${utl.kv("--datalakeBucket")}/temp/mrkt/${utl.db("work")}/${utl.kv("--appId")}")
    } else {
      //utl.deletePath(s"${utl.kv("--tempS3")}/mrkt/", true, true)
      argMap.put("--tempS3", s"${utl.kv("--tempS3")}/mrkt/${utl.db("work")}/${utl.kv("--appId")}")
    }
  }

  def deriveDates()(implicit spark: SparkSession, utl: SparkUtil, argMap: scala.collection.mutable.Map[String, String]): Unit = {                    
    
    val sql = if(utl.kv("--sysDate") != null) {
      s"""select date_format(eff_dt, '${utl.kv("--dateFormat")}') as eff_dt
               , ${utl.kv("--sysDate")} as sys_date
               , year(eff_dt)           as year
               , month(eff_dt)          as mnth
               , dayofyear(eff_dt)      as dayofyear
            from (select max(ib_effdate) as eff_dt
                    from ${utl.db("dw")}.inv_bal_current_fact
                   where ib_effdate <=  to_date('${utl.kv("--sysDate")}', '${utl.kv("--dateFormat")}')
                 ) ib
        """          
    } else 
      s""" select date_format(eff_dt, '${utl.kv("--dateFormat")}')               as eff_dt
                 , date_format(current_date(), '${utl.kv("--dateFormat")}')      as sys_date
                 , date_format(date_sub(eff_dt,31), '${utl.kv("--dateFormat")}') as sub_31_sk_date
                 , year(eff_dt)                                                  as year
                 , month(eff_dt)                                                 as mnth
                 , dayofyear(eff_dt)                                             as dayofyear
              from (select max(ib_effdate) as eff_dt
                      from ${utl.db("dw")}.inv_bal_current_fact
                   ) ib                      
      """
    
    val (bus_date: String, sys_date: String, sub_31_sk_date:String,bus_year: Int, bus_mnth: Int, dayofyear:Int) 
        = utl.sql(sql,0)
             .rdd
             .map(t => (t.getString(0), t.getString(1), t.getString(2), t.getInt(3), t.getInt(4), t.getInt(5))).collect()(0)

    val sub_35_sk_date = utl.sql(s"""
            select sk_date
              from ${utl.db("dw")}.date_dim
             where to_date(standard_date) = date_sub(to_date('${bus_date}', '${utl.kv("--dateFormat")}'),35)
        """,0).rdd.map(t => t.getDecimal(0).toString).collect()(0)

    SparkUtil.log(s"""Current System  date            : ${sys_date}""")
    SparkUtil.log(s"""Current Business date           : ${bus_date} , year: ${bus_year}, month: ${bus_mnth}, dayofyear:${dayofyear} """)
    SparkUtil.log(s"""Current Business Date - 31 Days : ${sub_31_sk_date}\n\n""")
    SparkUtil.log(s"""Current Business Date - 35 Days : ${sub_35_sk_date}\n\n""")
    
    argMap.put("bus_date", bus_date)
    argMap.put("sub_31_sk_date", sub_31_sk_date)
    argMap.put("sub_35_sk_date", sub_35_sk_date)
    argMap.put("sys_date", sys_date)
    argMap.put("bus_year", bus_year.toString)
    argMap.put("bus_mnth", bus_mnth.toString)
    argMap.put("bus_dayofyear", dayofyear.toString)
    
    
  }

  def main(argArr: Array[String]): Unit = {
    
  //println("build_date : 04-18-2018 version:1")
    implicit lazy val argMap = handleArgs(argArr)
    implicit lazy val spark: SparkSession = SparkUtil.getSparkSession(argMap)
    argMap.put("--appId", SparkUtil.getSparkAppId)
    implicit lazy val utl: SparkUtil = new SparkUtil()
     
     var ex: Throwable = null
    try {
      createWorkArea()
      if (utl.kv("--runEle").contains("all")  ||
          utl.kv("--runEle").contains("plan") ||
          utl.kv("--runEle").contains("part")) {
        deriveDates()
      }
      
      
      SparkUtil.log("Confs before we start" + s"\n${argMap.mkString("\n\t")}")

      if (utl.kv("--runEle").contains("all") ||
        utl.kv("--runEle").contains("plan")) {
        SparkUtil.log(s"\n\nStarted processing for Plan\n---------------------------------------------------------------- ")
        new Plans(utl).run
      } else {
        SparkUtil.log(s"Skipping the data element: plan")
      }

      if (utl.kv("--runEle").contains("all") ||
        utl.kv("--runEle").contains("part")) {
        SparkUtil.log(s"\n\nStarted processing for Part\n---------------------------------------------------------------- ")
        new Parts(utl).run
      } else {
        SparkUtil.log(s"Skipping the data element: Part")
      }
        
      if (utl.kv("--buildCSV").contains("all") ||
          utl.kv("--buildCSV").contains("plan")) {
         SparkUtil.log(s"\n\nBuilding the Plans csv file\n---------------------------------------------------------------- ")
         new PlansToCsv(utl).run
      }
      if (utl.kv("--buildCSV").contains("all") ||
          utl.kv("--buildCSV").contains("part")) {
         SparkUtil.log(s"\n\nBuilding the Parts csv file\n---------------------------------------------------------------- ")
         new PartsToCsv(utl).run
      }
      if (!( utl.kv("-retainWorkTbls", "nope") == "-retainWorkTbls" || utl.kv("--runMode") == "t" )) {
        SparkUtil.log(s"\n\nClearing the work Tables\n---------------------------------------------------------------- ")
        utl.clearWorkTables
      }
      
      SparkUtil.log(s"**********************Ending job**********************")
      
    } catch {
      case t: Throwable => {
        SparkUtil.log(s"**********************Failing job**********************")
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        println(sw.toString)
        sw.close
        throw new Exception(t)
      }
    }

  }

}

/*
 List of arguments,
 KV args,
   1. --logLevel     : 'WARN'  :- ['DEBUG' | 'INFO' | 'WARN' | 'ERROR'     ]
   2. --runMode      : 'p'     :- [ 'p'    | 't'    | 'r'                  ]        
   3. --runEle       : 'all'   :- [ 'all'  | 'plan' | 'part' | 'plan,part' ]
   4. --db-WORK      : work     
   5. --dateFormat   : 'yyyyMMdd'
   6. --appId
   7. --tempS3
   8. --db-DW         : 'prod_dw'
   9. --db-DM         : 'prod_dm'
  10. --db-EASY       : 'easy' 
  11. --db-ETL_RAPID  : 'etl_rapid' 
  12. --db-ETL_DM     : 'etl_dm'  
  13. --db-STATIC     : 'static'
  14. --tbl-PLANS     : 'plans'
  15. --tbl-PARTS     : 'parts'
  16. --s3Sec         : value for fs.s3a.secret.key
  17. --s3Acc         : value for fs.s3a.access.key
  18. --tstModule     : list of Class names to run (must match the case)
  19. --saveDFAs      : 'NONE'     :- ['HIVE' | 'S3' | 'HDFS' | 'CHECK_POINT']
  20. --rwInterval    : '5000' (5sec)
  21. --source_system : '' ['P_IN02'| 'P_PNP' | 'P_INST' | 'P_ISIS' ]
  22. --fileFormat    : 'PARQUET' [ 'PARQUET' | 'CSV' | 'ORC' | 'AVRO' ]
  23. --sgwBucket     : 's3://prod-gwf-datalake-storage-gateway-us-east-1'
  24. --buildCSV      : "plan,part"  [ 'all' | 'plan' | 'part' | 'plan,part']
  25. --hiveTblsAs    : "ext" [ 'ext' | 'mgd']
  26. --s3WaitIter    : 20    
  27. --outFileCnt    : 5
Boolean args
 1. -explainDF
 2. -saveDF 
 3. -useHint
 4. -overrideSparkConf
 5. -supressBroadcast
 6. -retainWorkTbls
 7. -printCounts
 8. -useHive4Work
 9. -buildParent    
10. -printDFCnt
11. -distinctCSVRecs   -> Do we need to add distinct clause while producing the CSV files
12. -printFiles prints the files created for hive table
13. -useS3ForHive  The Hive back end will be set as S3 instead of the HDFS. If HDFs, the Glue table will be created. However, Athena can't query the table
         
 */

package pub.ayada.scala.sparkUtils.cmn

object HDFSUtils {

    /**
      * pub.ayada.scala.sparkUtils.cmn.HDFSUtils.isHDFSFileExists(hdfsDirPath : String, sparkSession : org.apache.spark.sql.SparkSession) : Boolean = {
      */
    def isHDFSFileExists(hdfsDirPath : String, sparkContext : org.apache.spark.SparkContext) : Boolean = {
        org.apache.hadoop.fs.FileSystem.get(sparkContext.hadoopConfiguration).exists(new org.apache.hadoop.fs.Path(hdfsDirPath))
    }
}

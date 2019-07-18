package 

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import com.gwf.datalake.util.SparkUtil

class HDFSUtil (hdpCnf : Configuration) {
  private val fileSystem = FileSystem.get(hdpCnf)

  def delDirs(hdfsPath: String):Unit = { 
    val folderToDelete = new Path(hdfsPath)
    if (fileExists(folderToDelete)) {
      fileSystem.delete(folderToDelete, true)
    }
  }
  
  def delFile(hdfsPath: String): Unit = {
    val fileToDelete = new Path(hdfsPath)
    if (fileExists(fileToDelete)) {
      fileSystem.delete(fileToDelete, true)
    }
  }
  /** Checks if the file exists.
    *
    * @param hdfsPath the path of the file for which we check if it exists
    * @return if the file exists
    */
  def fileExists(hdfsPath: String): Boolean = {
    val fileToCheck = new Path(hdfsPath)
    fileSystem.exists(fileToCheck)
  }
  /** Checks if the file exists.
    *
    * @param hdfsPath the path of the file for which we check if it exists
    * @return if the file exists
    */
  def fileExists(hdfsPath: Path): Boolean = {
    fileSystem.exists(hdfsPath)
  }
  
  /** Lists folder names in the specified hdfs folder.
    *
    * {{{
    * assert(HdfsHelper.listFolderNamesInFolder("my/folder/path") == List("folder_1", "folder_2"))
    * }}}
    *
    * @param hdfsPath the path of the folder for which to list folder names
    * @return the list of folder names in the specified folder
    */
  def listSubFolders(hdfsPath: String): List[String] = {
    fileSystem
      .listStatus(new Path(hdfsPath))
      .filter(!_.isFile)
      .map(_.getPath.getName)
      .toList
      .sorted
  }
  
    def listFilesInFolder(
      hdfsPath: String,
      recursive: Boolean = false,
      onlyName: Boolean = true
  ): List[String] = {

    fileSystem
      .listStatus(new Path(hdfsPath))
      .flatMap(status => {

        // If it's a file:
        if (status.isFile) {
          if (onlyName) List(status.getPath.getName)
          else List(hdfsPath + "/" + status.getPath.getName)
        }
        // If it's a dir and we're in a recursive option:
        else if (recursive)
          listFilesInFolder(
            hdfsPath + "/" + status.getPath.getName,
            true,
            onlyName)
        // If it's a dir and we're not in a recursive option:
        else
          Nil
      })
      .toList
      .sorted
  }
  
}

object HDFSUtil {
  
  def main(argArr: Array[String]): Unit = {
    val argMap = com.gwf.datalake.util.SparkUtil.args2map(argArr)
    val spark = SparkSession
      .builder
      .appName("HDFSUtils")
      .enableHiveSupport
      .getOrCreate
      
    val utl = new HDFSUtil(spark.sparkContext.hadoopConfiguration)
    
    if (argMap.contains("--rmPath")) {
      for (f <- utl.listFilesInFolder(argMap.get("--rmPath").get)) {
          SparkUtil.log(f)
      }
      utl.delDirs(argMap.get("--rmPath").get)
    }
    
    
    
  }
}

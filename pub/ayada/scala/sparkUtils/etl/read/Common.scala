package pub.ayada.scala.sparkUtils.etl.read

import org.apache.spark.sql.DataFrame

import pub.ayada.scala.sparkUtils.cmn.UtilFuncs
import org.apache.spark.storage.StorageLevel
import pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs

object Common {

    def getTransformerDF(taskID : String,
                         defaults : scala.collection.mutable.Map[String, String],
                         props : pub.ayada.scala.sparkUtils.etl.read.ReadProps,
                         df : DataFrame) : (TransformerDFs, String) = {

        val msg = new StringBuilder()
        (new TransformerDFs(rePartitionAndPersist(taskID, df, props, defaults, msg).alias(props.id), 1, props.loadCount, null), msg.toString)

    }

    def rePartitionAndPersist(taskID : String,
                              df : DataFrame,
                              props : ReadProps,
                              defaults : scala.collection.mutable.Map[String, String],
                              msg : StringBuilder) : DataFrame = {
        if (props.broadcast) {
            msg.append("re-partitioning to 1 partition as the DF is marked to be broadcasted")
            UtilFuncs.rePartitionAndPersist(df, 1, if (props.forceNoPersist) StorageLevel.NONE else StorageLevel.MEMORY_ONLY)
        } else {
            val min = { if (props.repartition > 0) props.repartition else defaults.get("defaultNoOfDFPartitions").get.toInt }
            //   val res = UtilFuncs.calcOptimalPartitionCount(df, min)
            msg.append(s"For DF : ${props.id} trying to repartition to $min")
            UtilFuncs.rePartitionAndPersist(df, min, if (props.forceNoPersist) StorageLevel.NONE else StorageLevel.fromString(defaults.get("defaultStorageLevel").get))
        }
    }
}

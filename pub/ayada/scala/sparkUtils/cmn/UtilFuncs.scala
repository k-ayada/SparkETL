package pub.ayada.scala.sparkUtils.cmn

import java.util.Properties
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DataType, DataTypes }
import org.apache.spark.SparkContext
import pub.ayada.scala.utils.DateTimeUtils

object UtilFuncs {
    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.literal2SparkDataType(dataType : String) : DataType
      */
    def literal2SparkDataType(dataType : String) : DataType = {
        dataType.toLowerCase match {
            case "int"           => DataTypes.IntegerType
            case "integer"       => DataTypes.IntegerType
            case "intergertype"  => DataTypes.IntegerType
            case "long"          => DataTypes.LongType
            case "longtype"      => DataTypes.LongType
            case "floa"          => DataTypes.FloatType
            case "floattype"     => DataTypes.FloatType
            case "double"        => DataTypes.DoubleType
            case "decimal"       => DataTypes.DoubleType
            case "doubletype"    => DataTypes.DoubleType
            case "bool"          => DataTypes.BooleanType
            case "boolean"       => DataTypes.BooleanType
            case "booleantype"   => DataTypes.BooleanType
            case "nulltype"      => DataTypes.NullType
            case "byte"          => DataTypes.ByteType
            case "bytetype"      => DataTypes.ByteType
            case "datetime"      => DataTypes.TimestampType
            case "timestamp"     => DataTypes.TimestampType
            case "timestamptype" => DataTypes.TimestampType
            case "date"          => DataTypes.DateType
            case "datetype"      => DataTypes.DateType
            case _               => DataTypes.StringType
        }

    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.sql2SparkDataType(sqlType : Int) : DataType
      */
    def sql2SparkDataType(sqlType : Int) : DataType = {
        sqlType match {
            case java.sql.Types.INTEGER   => DataTypes.IntegerType
            case java.sql.Types.BOOLEAN   => DataTypes.BooleanType
            case java.sql.Types.SMALLINT  => DataTypes.ShortType
            case java.sql.Types.TINYINT   => DataTypes.ByteType
            case java.sql.Types.BIGINT    => DataTypes.LongType
            case java.sql.Types.DECIMAL   => DataTypes.createDecimalType
            case java.sql.Types.FLOAT     => DataTypes.FloatType
            case java.sql.Types.DOUBLE    => DataTypes.DoubleType
            case java.sql.Types.DATE      => DataTypes.DateType
            case java.sql.Types.TIME      => DataTypes.TimestampType
            case java.sql.Types.TIMESTAMP => DataTypes.TimestampType
            case java.sql.Types.VARCHAR   => DataTypes.StringType
            case _                        => DataTypes.StringType
        }
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPersistType(persistTypStr:String):StorageLevel
      */
    def getPersistType(persistTypStr : String) : StorageLevel = persistTypStr.toUpperCase match {
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
        case _                       => throw new IllegalArgumentException(s"Invalid StorageLevel: $persistTypStr")
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPersistTypeStr(persistTyp: StorageLevel):String
      */
    def getPersistTypeStr(persistTyp : StorageLevel) : String = persistTyp match {
        case StorageLevel.NONE                  => "NONE"
        case StorageLevel.DISK_ONLY             => "DISK_ONLY"
        case StorageLevel.DISK_ONLY_2           => "DISK_ONLY_2"
        case StorageLevel.MEMORY_ONLY           => "MEMORY_ONLY"
        case StorageLevel.MEMORY_ONLY_2         => "MEMORY_ONLY_2"
        case StorageLevel.MEMORY_ONLY_SER       => "MEMORY_ONLY_SER"
        case StorageLevel.MEMORY_ONLY_SER_2     => "MEMORY_ONLY_SER_2"
        case StorageLevel.MEMORY_AND_DISK       => "MEMORY_AND_DISK"
        case StorageLevel.MEMORY_AND_DISK_2     => "MEMORY_AND_DISK_2"
        case StorageLevel.MEMORY_AND_DISK_SER   => "MEMORY_AND_DISK_SER"
        case StorageLevel.MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
        case StorageLevel.OFF_HEAP              => "OFF_HEAP"
        case _                                  => persistTyp.description
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPswdFromAlias(props : Properties, sparkContext : SparkContext) : String
      *
      * Reads the value from the property key 'JDBCPasswordAlias' and retrieves the password from key store added to the spark Context spark.hadoop.hadoop.security.credential.provider.path
      */
    def getPswdFromAlias(props : Properties, sparkContext : SparkContext) : String = {

        if (!props.containsKey("JDBCPasswordAlias"))
            throw new Exception("JDBC connection property [JDBCPasswordAlias | JDBCPassword ] Not received.")
        if (!sparkContext.getConf.contains(("spark.hadoop.hadoop.security.credential.provider.path")))
            throw new Exception("Please pass keystore file path in the command line (--conf spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/<path to file>")

        try {
            new String(sparkContext.hadoopConfiguration.getPassword(props.getProperty("JDBCPasswordAlias")))
        } catch {
            case t : Throwable => throw new Exception("Failed to retrive password for alias: '" + props.getProperty("JDBCPasswordAlias") + "' from the kestore file saved in the keystore:hadoop.security.credential.provider.path", t)
        }
    }

    def getMemoryUtilization(sc : SparkContext) : String = {
        val sb = new StringBuffer()
        val executorMemoryStatus = sc.getExecutorMemoryStatus
        sb.append("  ExecutorMemoryStatus:\n")
        for ((name, mem) <- executorMemoryStatus) {
            sb.append("    - Name: '").append(name).append("'\n")
            sb.append("      MaxMemForCaching: ").append(mem._1).append("'\n")
            sb.append("      RemainingMemForCaching: ").append(mem._2).append("'\n")
        }
        val storageInfo = sc.getRDDStorageInfo
        sb.append("  StorageInfoLength: " + storageInfo.size)
        if (storageInfo.size > 0) {
            sb.append("  StorageInfo:\n")
            for (info <- storageInfo) {
                sb.append(s"    - name: ${info.name}\n")
                sb.append(s"      memSize: ${info.memSize}\n")
                sb.append(s"      diskSize: ${info.diskSize}\n")
                sb.append(s"      numPartitions: ${info.numPartitions}\n")
                sb.append(s"      numCachedPartitions: ${info.numCachedPartitions}\n")
                sb.append(s"      storageLevel: ${info.storageLevel}\n")
            }
        }
        sb.toString
    }

    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.isFileExists(flPath : String, sparkContext : SparkContext) : Boolean = {
      */
    def isFileExists(flPath : String, sparkContext : SparkContext) : Boolean = {
        var res = false
        if (flPath.startsWith("s3n://") || flPath.startsWith("s3://") || flPath.startsWith("s3a://")) {
            val splits : Array[String] = flPath.split("/")
            val protocol : String = splits(0) + "://"
            val bucket : String = splits(2)
            val path = splits.takeRight(splits.length - 3).mkString("/")
            res = pub.ayada.scala.sparkUtils.cmn.AWSUtils.isS3FileExists(protocol, bucket, path, sparkContext)
        } else if (flPath.startsWith("hdfs://")) {
            res = pub.ayada.scala.sparkUtils.cmn.HDFSUtils.isHDFSFileExists(flPath, sparkContext)
        }
        res
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPropOrElse(props : Properties, key : String, default : String)
      */
    def getPropOrElse(props : Properties, key : String, default : String) : String = {
        if (props.containsKey(key)) {
            return props.getProperty(key)
        }
        default
    }
    /**
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPropOrFail(props : Properties, key : String)
      */
    def getPropOrFail(props : Properties, key : String) : String = {

        if (!props.containsKey(key))
            throw new Exception("JDBC connection property '" + key + "' Not received.")
        props.getProperty(key)
    }
    /**
      * If re-partition is requested, and the number of partitions are less than 1900,
      * use coalesce function to re-partition the DF to 2048 partitions
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.rePartitionDF(df : DataFrame, count : Int)
      */
    def rePartitionDF(df : DataFrame, count : Int) : DataFrame = {
        if (df.rdd.partitions.size > count)
            df
        else
            df.coalesce(if (count < 2000 && count > 19000) 2048 else count)
    }

    /**
      * If the partition memory usage is > 64MB(67108864 bytes) use newPartitions = ( cur_partitions * 64MB / cur_partition_capacity ) * 2
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.calcOptimalPartitionCount(df : DataFrame, minPartions : Int)
      */
    def calcOptimalPartitionCount(df : DataFrame, minPartions : Int) : (Int, Int, Int) = {
        val InitialParts = df.rdd.partitions.size
        val cap = org.apache.spark.util.SizeEstimator.estimate(df.rdd.partitions(0)).toInt

        val size = { InitialParts * cap / 67108864 }.ceil.toInt

        if ((InitialParts < minPartions && cap < 67108864))
            (InitialParts, cap, InitialParts)
        else if (size > minPartions)
            (InitialParts, cap, size)
        else (InitialParts, cap, minPartions)
    }

    /**
      * Persists the DF and returns the new DF
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.persistDF(df : DataFrame,
      * persist : StorageLevel)
      */
    def persistDF(df : DataFrame,
                  persist : StorageLevel) : DataFrame = {
        val size = df.rdd.partitions.size
        if (null != persist && persist != StorageLevel.NONE) {
            df.persist(persist)
        } else { df }
    }
    /**
      * If number of Parts is forced as 1, persists the DF in MEMORY_ONLY else in MEMORY_AND_DISK
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.persistDF(df : DataFrame,
      * broadcast : Boolean, repartition : Int)
      */
    def rePartitionAndPersist(df : DataFrame, repartition : Int) : DataFrame = {
        persistDF(rePartitionDF(df, repartition),
            {
                if (repartition == 1)
                    StorageLevel.MEMORY_ONLY
                else
                    StorageLevel.MEMORY_AND_DISK
            })
    }
    /**
      * If number of Parts is forced as 1, persists the DF in MEMORY_ONLY else in MEMORY_AND_DISK
      * pub.ayada.scala.sparkUtils.cmn.UtilFuncs.persistDF(df : DataFrame,
      * broadcast : Boolean, repartition : Int)
      */
    def rePartitionAndPersist(df : DataFrame, repartition : Int, storageLevel : StorageLevel) : DataFrame = {
        persistDF(rePartitionDF(df, repartition),
            {
                if (repartition == 1)
                    StorageLevel.MEMORY_ONLY
                else
                    storageLevel
            })
    }

}

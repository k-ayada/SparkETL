package pub.ayada.scala.sparkUtils.etl.read.csv

import org.apache.spark.sql.types.StructType

case class Csv2DFProps(taskType : String = "Csv2DF",
                       id : String,
                       propsFile : String,
                       printSchema : Boolean = true,
                       loadCount : Boolean = false,
                       path : String,
                       header : Boolean = true,
                       delimiter : String = ",",
                       quote : String = "\"",
                       escape : String = "\\",
                       mode : String = "PERMISSIVE",
                       charset : String = "UTF-8",
                       commentChar : String = null.asInstanceOf[String],
                       dateFormat : String = "M/d/yyyy H:m:s a",
                       nullValue : String,
                       repartition : Int = 0,
                       broadcast : Boolean = false,
                       forceNoPersist : Boolean = false,
                       csvColumns : StructType) extends pub.ayada.scala.sparkUtils.etl.read.ReadProps

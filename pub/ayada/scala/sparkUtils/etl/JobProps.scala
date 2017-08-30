package pub.ayada.scala.sparkUtils.etl

import scala.xml.NodeSeq

import org.apache.spark.SparkContext

import pub.ayada.scala.sparkUtils.etl.read.ReadProps
import pub.ayada.scala.sparkUtils.etl.read.csv.Csv2DF
import pub.ayada.scala.sparkUtils.etl.read.jdbc.Jdbc2DF
import pub.ayada.scala.sparkUtils.etl.read.hive.Hive2DF
import pub.ayada.scala.sparkUtils.etl.write.WriteProps
import pub.ayada.scala.sparkUtils.etl.write.hive.DF2Hive
import pub.ayada.scala.sparkUtils.etl.write.parquet.DF2Parquet
import pub.ayada.scala.sparkUtils.etl.transform.{ Transformer, TransformerDFs, TransformerProps }

case class JobProps(
    id : String,
    writeProps : List[(String, WriteProps)],
    transformProps : List[TransformerProps],
    readProps : List[(String, ReadProps)])

object JobProps {

    def getSrc2DFProps(srcNodes : NodeSeq,
                       sparkContext : SparkContext,
                       defaults : scala.collection.mutable.Map[String, String],
                       logs : StringBuilder) : List[(String, ReadProps)] = {

        var readProps : scala.collection.mutable.ListBuffer[(String, ReadProps)] = scala.collection.mutable.ListBuffer()

        (srcNodes \ "_").foreach(read => {
            val typ = read.label.toLowerCase
            logs.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main-JobProps"))
                .append("Processing Source: '")
                .append((read \ "@id").text)
                .append("'. Type: ")
                .append(typ).append("\n")
            typ match {
                case "csv" =>
                    readProps += { ("csv", Csv2DF.getCsv2DFProp(read, defaults, logs)) }
                case "jdbc" =>
                    readProps += { ("jdbc", Jdbc2DF.getJdbc2DFProp(read, sparkContext, defaults, logs)) }
                case "hive" =>
                    readProps += { ("hive", Hive2DF.getHive2DFProp(read, defaults, logs)) }
                case _ =>
                    logs.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main-JobProps"))
                        .append("Err: Unknown source type. Skipping this source.\n")
                        .append(new scala.xml.PrettyPrinter(250, 4).format(read))
            }

        })
        readProps.toList
    }

    def getDF2TargetProps(destNodes : NodeSeq,
                          sparkContext : SparkContext,
                          defaults : scala.collection.mutable.Map[String, String],
                          logs : StringBuilder) : List[(String, WriteProps)] = {

        var writeProps : scala.collection.mutable.ListBuffer[(String, WriteProps)] = scala.collection.mutable.ListBuffer()

        (destNodes \ "_").foreach(write => {
            val typ = write.label.toLowerCase
            logs.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main-JobProps"))
                .append("Processing Destination: '")
                .append((write \ "@id").text)
                .append("'. Type: ")
                .append(typ).append("\n")
            typ match {
                case "hive" =>
                    writeProps += { ("hive", DF2Hive.getDF2HiveProps(write, logs)) }
                case "parquet" =>
                    writeProps += { ("jdbc", DF2Parquet.getDF2ParquetProps(write, logs)) }
                case _ =>
                    logs.append(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm("Driver-Main-JobProps"))
                        .append("Err: Unknown Target type. Skipping this Target.\n")
                        .append(new scala.xml.PrettyPrinter(250, 4).format(write))
            }

        })
        writeProps.toList
    }

    def getTransformers(xferNode : NodeSeq,
                        defaults : scala.collection.mutable.Map[String, String]) : List[TransformerProps] = {
        var res : List[TransformerProps] = List()
        (xferNode \ "transformer").foreach(x => {
            res = res :+ Transformer.getTransformerProps(x, defaults)
        })
        res
    }
}

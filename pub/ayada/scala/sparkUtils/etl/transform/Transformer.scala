package pub.ayada.scala.sparkUtils.etl.transform

import scala.xml.Node
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

import pub.ayada.scala.utils.DateTimeUtils
import pub.ayada.scala.sparkUtils.cmn.UtilFuncs

object Transformer {
    def getTransformerProps(xFerNode : Node,
                            defaults : scala.collection.mutable.Map[String, String]) : pub.ayada.scala.sparkUtils.etl.transform.TransformerProps = {
        val Id = (xFerNode \ "@id").text.trim
        val SrcDF = (xFerNode \ "@srcDF").text.trim.split(",")
        val Typ = (xFerNode \ "@type").text.trim
        val RegisterTempTable = ("true" == (xFerNode \ "@registerTempTable").text.trim.toLowerCase)
        val Repartition = (xFerNode \ "@repartition").text.trim
        val Persist = (xFerNode \ "@persist").text.trim.toUpperCase
        val LoadCount = ("true" == (xFerNode \ "@loadCount").text.trim.toLowerCase)
        val PrintSchema = ("true" == (xFerNode \ "@printSchema").text.trim.toLowerCase)
        val ConditionExpr = xFerNode.text.trim
        new pub.ayada.scala.sparkUtils.etl.transform.TransformerProps(
            id = Id,
            srcDF = SrcDF,
            typ = Typ,
            registerTempTable = RegisterTempTable,
            persist = StorageLevel.fromString(if ("" != Persist) Persist else defaults.get("defaultStorageLevel").get),
            repartition = { if ("" == Repartition) 0 else Repartition.toInt },
            loadCount = LoadCount,
            printSchema = PrintSchema,
            conditionExpr = ConditionExpr)
    }

    def runTransforms(jobId : String,
                      hiveContext : HiveContext,
                      props : List[pub.ayada.scala.sparkUtils.etl.transform.TransformerProps],
                      defaults : scala.collection.mutable.Map[String, String],
                      dfs : scala.collection.mutable.Map[String, pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs]) : scala.collection.mutable.ListBuffer[pub.ayada.scala.sparkUtils.etl.transform.TransformerProps] = {

        val parkedTransformations : scala.collection.mutable.ListBuffer[pub.ayada.scala.sparkUtils.etl.transform.TransformerProps] = scala.collection.mutable.ListBuffer()

        props.foreach(prop => {
            prop.srcDF.filter(_ != "").foreach(s => {
                var df : pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs = dfs.getOrElse(s, null)
                if (null != df) df.useCount += 1
            })
        })

        props.foreach(prop => {
            val taskID = s"Transformer-${jobId}-${prop.id}"
            try {
                println(DateTimeUtils.getFmtDtm(taskID) + "Prepping Transformarmation: " + prop.toString)
                var cont = true
                prop.srcDF.filter(_ != "").foreach({ srcDF =>
                    val paentDF = dfs.getOrElse(srcDF, null)
                    if (cont && !dfs.contains(srcDF)) {
                        cont = false
                        println(DateTimeUtils.getFmtDtm(taskID) + "Wrn: Source DataFrame: '" + srcDF + "' not found. Parking current transformation")
                        parkedTransformations += prop
                    }
                })

                if (cont) {
                    var df2 : DataFrame = null
                    prop.typ match {
                        case "filter" => {
                            val df = dfs.getOrElse(prop.srcDF(0), null)
                            if (df == null) {
                                println(DateTimeUtils.getFmtDtm(taskID) + " Failed to retrieve source DF to run the filter : " + prop.srcDF(0))
                            }
                            df2 = df.df.filter(prop.conditionExpr).alias(prop.id)
                        }
                        case "sql" => {
                            df2 = hiveContext.sql(prop.conditionExpr).alias(prop.id)
                        }
                        case _ => throw new Exception("Unknown transformation type received (valid:filter|sql) : " + prop.typ)
                    }

                    if (df2 == null) {
                        println(DateTimeUtils.getFmtDtm(taskID) + s" DdataFrame is null : ${prop.id}")
                    }
                    println(DateTimeUtils.getFmtDtm(taskID) + s" Registering DF as temp table: ${prop.id}")

                    if (prop.printSchema) {
                        println(DateTimeUtils.getFmtDtm(taskID) + s"DF schema of : ${prop.id} ${df2.schema.treeString}")
                    }

                    var useCnt = 0;
                    props.foreach(p => {
                        p.srcDF.filter(_ != "").foreach(s => {
                            if (s == prop.id)
                                useCnt += 1
                        })
                    })
                    if (useCnt > 1 || (prop.persist != null && prop.persist != StorageLevel.NONE)) {

                        val min = { if (prop.repartition > 0) prop.repartition else defaults.get("defaultNoOfDFPartitions").get.toInt }

                        val res = UtilFuncs.calcOptimalPartitionCount(df2, min)
                        println(pub.ayada.scala.utils.DateTimeUtils.getFmtDtm(taskID) +
                            s"For DF : ${prop.id} Derived new Partition counts as newPartitions =  ( cur_partitions/ cur_partition_capacity * 64MB  ) or $min ==> ${res._3} . Calculation = ( ${res._1} / ${res._2 / 1024 / 1024}MB * 64MB)"
                        )
                        val df3 = UtilFuncs.rePartitionAndPersist(df2, res._3, prop.persist)

                        df3.registerTempTable(prop.id)
                        dfs.put(prop.id, new pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs(df3, useCnt, prop.loadCount, StorageLevel.NONE))
                    } else {
                        df2.registerTempTable(prop.id)
                        dfs.put(prop.id, new pub.ayada.scala.sparkUtils.etl.transform.TransformerDFs(df2, useCnt, prop.loadCount, prop.persist))
                    }
                }
            } catch {
                case t : Exception => {
                    println(DateTimeUtils.getFmtDtm(taskID) + " Failed to complete the task for transformation ID: " + prop.id)
                    t.printStackTrace()
                    throw new Exception(t)
                }
            }
        })

        parkedTransformations
    }
}

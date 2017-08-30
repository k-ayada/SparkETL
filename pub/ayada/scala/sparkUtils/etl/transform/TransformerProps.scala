package pub.ayada.scala.sparkUtils.etl.transform

import org.apache.spark.storage.StorageLevel
case class TransformerProps(id : String,
                            srcDF : Array[String],
                            typ : String,
                            registerTempTable : Boolean = true,
                            persist : StorageLevel = StorageLevel.NONE,
                            repartition : Int = 0,
                            loadCount : Boolean = false,
                            printSchema : Boolean = false,
                            conditionExpr : String) {
    override def toString() : String = {
        new StringBuilder()
            .append("TransformerProps(")
            .append("id->").append(id)
            .append(", srcDFs->").append(srcDF.mkString("[", ",", "]"))
            .append(", registerTempTable->").append(registerTempTable)
            .append(", persist->").append(pub.ayada.scala.sparkUtils.cmn.UtilFuncs.getPersistTypeStr(persist))
            .append(", repartition->").append(repartition)
            .append(", loadCount->").append(loadCount)
            .append(", printSchema->").append(printSchema)
            .append(", conditionExpr->").append(conditionExpr.split("\n").map(_.trim).mkString(" "))
            .append(")").toString
    }

}

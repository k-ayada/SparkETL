package pub.ayada.scala.sparkUtils.etl.transform

case class TransformerDFs(var df : org.apache.spark.sql.DataFrame = null,
                          var useCount : Int = 0,
                          var loadCount : Boolean = false,
                          var persist : org.apache.spark.storage.StorageLevel) {
    override def toString() : String = {
        new StringBuilder()
            .append("TransformerDFs(")
            .append("df->").append({ df == null })
            .append(", useCount->").append(useCount)
            .append(", loadCount->").append(loadCount)
            .append(", persist->").append(persist)
            .append(")").toString
    }
}

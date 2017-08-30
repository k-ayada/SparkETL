package pub.ayada.scala.sparkUtils.etl.read.hive

case class Hive2DFProps(taskType : String = "Hive2DF",
                        id : String,
                        propsFile : String = null,
                        schema : String = "default",
                        table : String,
                        repartition : Int = 0,
                        broadcast : Boolean = false,
                        loadCount : Boolean = false,
                        printSchema : Boolean = true,
                        forceNoPersist : Boolean = false,
                        sql : String) extends pub.ayada.scala.sparkUtils.etl.read.ReadProps {

    override def toString() : String = {
        new StringBuilder()
            .append("taskType ->").append(taskType)
            .append(", id ->").append(id)
            .append(", schema ->").append(schema)
            .append(", table ->").append(table)
            .append(", loadCount ->").append(loadCount)
            .append(", printSchema ->").append(printSchema)
            .append(", sql ->").append(sql.split("\n").map(_.trim).mkString(" "))
            .toString
    }
}

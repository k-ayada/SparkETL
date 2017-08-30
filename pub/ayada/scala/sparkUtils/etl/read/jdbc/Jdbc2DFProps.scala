package pub.ayada.scala.sparkUtils.etl.read.jdbc

case class Jdbc2DFProps(taskType : String = "Jdbc2DF",
                        id : String,
                        propsFile : String,
                        loadCount : Boolean = false,
                        printSchema : Boolean = true,
                        schema : String,
                        table : String,
                        fetchSize : String = "10000",
                        isolationLevel : String = "READ_COMMIT",
                        repartition : Int = 0,
                        broadcast : Boolean = false,
                        sql : String,
                        partitionKey : String,
                        noOfPartitions : String,
                        lowerBound : String,
                        upperBound : String,
                        forceNoPersist : Boolean = false,
                        connParms : scala.collection.mutable.Map[String, String]) extends pub.ayada.scala.sparkUtils.etl.read.ReadProps {

    override def toString() : String = {

        new StringBuilder()
            .append("taskType ->").append(taskType)
            .append(", id ->").append(id)
            .append(", propsFile ->").append(propsFile)
            .append(", printSchema ->").append(printSchema)
            .append(", schema ->").append(schema)
            .append(", table ->").append(table)
            .append(", fetchSize ->").append(fetchSize)
            .append(", isolationLevel ->").append(isolationLevel)
            .append(", repartition ->").append(repartition)
            .append(", partitionColumn ->").append(partitionKey)
            .append(", noOfPartitions ->").append(noOfPartitions)
            .append(", lowerBound ->").append(lowerBound)
            .append(", upperBound ->").append(upperBound)
            .append(", forceNoPersist ->").append(forceNoPersist)
            .append(", connParms ->").append({
                for (k <- connParms.keys) {
                    if ("password" == k) { k + " -> ********" }
                    else { k + " -> " + connParms.getOrElse(k, "") }
                }
            })
            .append(", sql ->").append(sql.split("\n").map(_.trim).mkString(" "))
            .toString
    }
}

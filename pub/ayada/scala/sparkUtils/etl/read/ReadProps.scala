package pub.ayada.scala.sparkUtils.etl.read

trait ReadProps {
    def taskType : String
    def id : String
    def propsFile : String
    def printSchema : Boolean = true
    def loadCount : Boolean = false
    def repartition : Int = 0
    def broadcast : Boolean = false
    def forceNoPersist : Boolean = false
}

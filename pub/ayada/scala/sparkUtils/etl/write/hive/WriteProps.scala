package pub.ayada.scala.sparkUtils.etl.write

trait WriteProps {
    def taskType : String
    def id : String
    def srcDF : String
    def loadType : String
    def partitionColumns : Array[String]
    def preLoadCount : Boolean
    def postLoadCount : Boolean
}

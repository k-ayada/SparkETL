package pub.ayada.scala.sparkUtils.interpreter
import scala.tools.nsc.interpreter.Results.Success
import scala.xml.NodeSeq
import org.apache.spark.sql.functions.udf

trait Udfs {
    def execute
}

object InterpreterUtil {

    def main(args : Array[String]) : Unit = {

        val stringStream : java.io.StringWriter = new java.io.StringWriter()
        val imain : scala.tools.nsc.interpreter.IMain = getInterpritter(stringStream);
        imain.initializeSynchronous

        println("Setting:" + imain.isettings.toString())

        val _res : AnyRef = null
        imain.bind("_res", _res)
        val code : String = """import org.apache.spark.sql.functions.udf
                               class TestApp extends UDFs{
                                def test(arg: String): String = {
                                    "Hello " + arg
                                }
                             }
                             val _res:TestApp = new TestApp
                             """

        runScriptString(code, imain) match {
            case scala.tools.nsc.interpreter.Results.Success => {
                //println(getValue(imain, "_res").test("Kiran!!! :) ") + "\n\n")

                println(stringStream)
            }
            case _ => {
                println(stringStream)
                throw new Exception("Failed to execute IMain")
            }

        }

    }

    def getValue(imain : scala.tools.nsc.interpreter.IMain, name : String) : AnyRef = {
        imain.valueOfTerm(name).getOrElse(null.asInstanceOf[AnyRef])
    }

    def getInterpritter(stringStream : java.io.StringWriter) : scala.tools.nsc.interpreter.IMain = {
        val settings : scala.tools.nsc.Settings = new scala.tools.nsc.Settings

        //val param : List[String] = scala.collection.immutable.List.make(1, "true")
        //settings.usejavacp.tryToSet(param)
        val stream : java.io.PrintWriter = new java.io.PrintWriter(stringStream)

        new scala.tools.nsc.interpreter.IMain(settings, stream)
    }

    def runScriptString(script : String, imain : scala.tools.nsc.interpreter.IMain) : scala.tools.nsc.interpreter.Results.Result = {
        val res : scala.tools.nsc.interpreter.Results.Result = imain.interpret(script)
        res
    }

    def getFunctionMap(funcStrings : NodeSeq) : AnyRef = {
        val wrapper = new StringBuilder()
        wrapper.append("class UsrFuncs {\n")
        wrapper.append("""\n}
                        val usrFuncs:UsrFuncs = new UsrFuncs
                       """)
        null

    }

}

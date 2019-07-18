package com.gwf.datalake.util

import java.io.StringWriter
import java.io.PrintWriter

object ScalaUtil {
  def getStackTraceAsStr(t: Throwable): String = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString()
  }
}

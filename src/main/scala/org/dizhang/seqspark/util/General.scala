package org.dizhang.seqspark.util

/**
  * some useful functions
  */
object General {

  implicit class RichDouble(val x: Double) extends AnyVal {
    def sqrt = math.pow(x, 0.5)
    def square = math.pow(x, 2.0)
    def cube = math.pow(x, 3.0)
  }

  def max[A](x: A, y: A)(implicit ordering: Ordering[A]): A = {
    ordering.compare(x, y) match {
      case -1 => y
      case _ => x
    }
  }

  def max[A](elems: A*)(implicit ordering: Ordering[A]): A = {
    elems.max
  }

  def badArgs(args: Array[String]): Boolean = {
    if (args.length != 1) {
      true
    } else {
      false
    }
  }

  def usage(mode: String): Unit = {
    val mesg = s"""
                |spark-submit --class $mode [options] /path/to/wesqc.xx.xx.jar project.conf
                |
                |   options:         Spark options, e.g. --num-executors, please refer to the spark documentation.
                |
                |   project.conf:    The configuration file in INI format, could be other name.

               """
    print(mesg)
  }

}

package org.dizhang.seqa.annot

import org.dizhang.seqa.util.Constant.Annotation._

/**
  * Gene
  */
object Gene {

}

trait Gene {
  val name: String
  val locations: Array[Location]
}

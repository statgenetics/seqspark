package org.dizhang.seqa.annot

/**
  * Gene contains mRNAs and Locations
  */
@SerialVersionUID(102L)
class Gene(val name: String,
           val mRNA: Array[mRNA],
           val loci: Array[Location]) extends Serializable {

}

object Gene {

}
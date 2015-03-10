class Titv(arg1: Array[Int], arg2: Array[Int]) {
  val ti = arg1
  val tv = arg2
  def + (that: Titv): Titv = {
    for (i <- Range(0, ti.length)) {
      ti(i) += that.ti(i)
      tv(i) += that.tv(i)
    }
    this
    /*
    val newTi = for {
      indiTi1 <- ti
      indiTi2 <- that.ti
    } yield indiTi1 + indiTi2
    val newTv = for {
      indiTv1 <- tv
      indiTv2 <- that.tv
    } yield indiTv1 + indiTv2
    new Titv(newTi, newTv)
    */
  }

  //override def toString = ti + "\n" + tv
    //ti.zip(tv).map(x => x._1 + " " + x._2 + "\n").reduce((a, b) => a + b)
}


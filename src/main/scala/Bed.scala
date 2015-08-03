import org.apache.spark.AccumulableParam
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(4L)
class Bed (arg1: Array[Byte], arg2: Array[Byte]) extends Serializable {
  /**
    * This class is for holding the output buf,
    * we don't care about random access, just use two
    * Byte arrays for bim and bed
    */
  val bim = arg1
  val bed = arg2
}

object Bed {
  import Constant._
  def apply(): Bed = {
    new Bed(Array[Byte](), Array[Byte]())
  }
  def apply(v: Variant[Byte]): Bed = {
    def make (g: Byte): Byte = {
      //val s = g split (":")
      g match {
        case Bt.mis => 1.toByte
        case Bt.ref => 0.toByte
        case Bt.het => 2.toByte
        case Bt.mut => 3.toByte
        case _ => 1.toByte
      }
    }
    val id = "%s-%s" format(v.chr, v.pos)
    val bim: Array[Byte] = 
      ( "%s\t%s\t%d\t%s\t%s\t%s\n"
        .format(v.chr,id,0,v.pos,v.ref,v.alt)
        .toArray
        .map(_.toByte) )
    val bed: Array[Byte] =
      for {
        i <- Array[Int]() ++ (0 to v.geno.length/4)
        four = 0 to 3 map (j => if (4 * i + j < v.geno.length) make(v.geno(4 * i + j)) else 0.toByte)
      } yield
        four.zipWithIndex.map(a => a._1 << 2 * a._2).sum.toByte
    new Bed(bim, bed)
  }
  def add(a: Bed, b: Bed): Bed = {
    new Bed(a.bim ++ b.bim, a.bed ++ b.bed)
  }
}

/**
@SerialVersionUID(9L)
class Tped (c: Byte, p: Int, g: Array[Byte]) extends Serializable {
  val chr = c
  val pos = p
  def geneticDis = 0
  def identifier = "%s-%s" format (chr, pos)
  val geno: Array[Byte] = g
  override def toString = {
    "%s %s %s %s %s" format (chr, this.identifier, this.geneticDis, pos, geno.map(x => if (x != 0) x.toChar else '0').mkString(" "))
  }
}

object Tped {
  def apply (v: Variant[String]): Tped = {
    val num = "(\\d+)".r
    val c = v.chr match {
      case num(x) => x.toByte
      case "X" => 23.toByte
      case "Y" => 24.toByte
      case "XY" => 25.toByte
      case "M" => 26.toByte
      case "MT" => 27.toByte
      case _ => 0.toByte
    }
    val gs: Array[Byte] =
      v.geno .flatMap { g: String =>
        g match {
          case "./." => List(0.toByte, 0.toByte)
          case "0/0" => List(v.ref(0).toByte, v.ref(0).toByte)
          case "0/1" => List(v.ref(0).toByte, v.alt(0).toByte)
          case "1/1" => List(v.alt(0).toByte, v.alt(0).toByte)
          case _ => List(0.toByte, 0.toByte)
        }
      }
    new Tped(c, v.pos, gs.toArray)
  }
}



  * object BedAccumulableParam extends AccumulableParam[Array[Bed], Bed] {
  * type Res = Array[Bed]
  * def addAccumulator(r: Res, t: Bed): Res = {
  *   r :+ t
  * }
  * def addInPlace(r1: Res, r2: Res): Res = {
  *   r1 ++ r2
  * }
  * def zero(initialValye: Res): Res = {
  *   Array[Bed]()
  * }
  * }
  */

/**
class Bed (v: Variant[String], make: String => Byte) extends Serializable {
  val chr: ArrayBuffer[Byte] =
    ArrayBuffer[Byte](
      v.chr match {
        case "X" => 23.toByte
        case "Y" => 24.toByte
        case "XY" => 25.toByte
        case "M" => 26.toByte
        case s: String => s.toByte
        case _ => 0.toByte
      }
    )
  //val snp = "%s-%d" format (v.chr, v.pos)
  //val gDis = 0
  val pos = ArrayBuffer[Int](v.pos)
  val a1 = ArrayBuffer[Byte](v.ref(0).toByte)
  val a2 = ArrayBuffer[Byte](v.alt(0).toByte)
  val cnt: ArrayBuffer[Byte] =
    for {
      i <- ArrayBuffer[Int]() ++ (0 to v.cnt.length/4)
      four = 0 to 3 map (j => if (4 * i + j < v.cnt.length) make(v.cnt(4 * i + j)) else 0.toByte)
    } yield
      four.zipWithIndex.map(a => a._1 << 2 * a._2).sum.toByte
}

object Bed {
  def apply(v: Variant[String], make: String => Byte): Bed = {
    new Bed(v, make)
  }
  def add (a: Bed, b: Bed): Bed = {
    a.chr ++= b.chr
    a.pos ++= b.pos
    a.a1 ++= b.a1
    a.a2 ++= b.a2
    a.cnt ++= b.cnt
    a
  }
}
  */

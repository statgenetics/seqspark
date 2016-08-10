package org.dizhang.seqspark.geno

/**
  * Okay, let's try to do some functional programming here
  * how to interpret the DSL we define for the genotype data
  */
trait Interpreter[A]

object Interpreter {

  /** the default interpreter is to evaluate */
  case class Eval[A](value: A) extends Interpreter[A]
  def eval[A]: Eval[A] => A = _.value
}

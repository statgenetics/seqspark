package org.dizhang.seqspark.util

import scala.language.implicitConversions

/**
 * borrow from http://stackoverflow.com/questions/4199393/are-options-and-named-default-arguments-like-oil-and-water-in-a-scala-api/4199579#4199579
 * Aaron Novstrup's answer
 * So we can define real optional parameters, without using Option()/Some() on the client side
 */

class Opt[A] private (val option: Option[A])
object Opt {
  implicit def any2opt[A](a: A): Opt[A] = new Opt(Option(a))
  implicit def option2opt[A](o: Option[A]): Opt[A] = new Opt(o)
  implicit def opt2option[A](o: Opt[A]): Option[A] = o.option
}

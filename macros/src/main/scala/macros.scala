package ygg

import scala.util._

package object macros {
  def doTry[A](body: => A): Try[A] = Try(body)

  implicit class TryOps[A](private val x: Try[A]) extends AnyVal {
    def |(expr: => A): A = fold(_ => expr, x => x)
    def fold[B](f: Throwable => B, g: A => B): B = x match {
      case Success(x) => g(x)
      case Failure(t) => f(t)
    }
  }
}

package macros {
  import ygg.json._

  object Json {
    implicit final class JsonStringContext(sc: StringContext) {
      def json(args: Any*): JValue             = macro ygg.macros.JsonMacros.jsonInterpolatorImpl
      def jsonMany(args: Any*): Vector[JValue] = macro ygg.macros.JsonMacros.jsonManyInterpolatorImpl
    }
  }
}

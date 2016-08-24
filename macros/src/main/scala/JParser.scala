package ygg.macros

import jawn._, AsyncParser._
import scala.util._

object JParser {
  type ResultSeq[A] = Either[ParseException, Seq[A]]

  def stream[A](implicit z: Facade[A]): AsyncParser[A] = Parser.async[A](ValueStream)
  def json[A](implicit z: Facade[A]): AsyncParser[A]   = Parser.async[A](SingleValue)
  def unwrap[A](implicit z: Facade[A]): AsyncParser[A] = Parser.async[A](UnwrapArray)

  def parseUnsafe[A](str: String)(implicit z: Facade[A]): A = Parser.parseUnsafe[A](str)

  def parse[A](str: String)(implicit z: Facade[A]): Try[A]          = Try(parseUnsafe[A](str))
  def parseMany[A](str: String)(implicit z: Facade[A]): Try[Seq[A]] = exhaust(stream[A])(_ absorb str)

  private def exhaust[A](p: AsyncParser[A])(f: AsyncParser[A] => ResultSeq[A])(implicit z: Facade[A]): Try[Seq[A]] =
    ( for (r1 <- f(p).right ; r2 <- p.finish.right) yield r1 ++ r2 ) match {
      case Left(t)  => Failure(t)
      case Right(x) => Success(x)
    }
}

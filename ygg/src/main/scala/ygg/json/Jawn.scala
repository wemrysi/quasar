package ygg.json

import jawn._
import jawn.AsyncParser._
import blueeyes.ByteBuffer
import blueeyes.json.JValue

case class AsyncParse[A](errors: Seq[ParseException], values: Seq[A])

sealed trait ParseResult[A] {
  def errors: Seq[Exception]
  def values: Seq[A]
  def next: AsyncParser[A]
}
object ParseResult {
  def unapply[A](x: ParseResult[A]) = Some((x.errors, x.values, x.next))
}
final case class ParseError[A](errors: Seq[Exception], next: AsyncParser[A]) extends ParseResult[A] {
  def values = Nil
}
final case class ParseValues[A](values: Seq[A], next: AsyncParser[A]) extends ParseResult[A] {
  def errors = Nil
}

object AsyncParser {
  type P = AsyncParser[JValue]

  def stream(): P = Parser.async[JValue](ValueStream)
  def json(): P   = Parser.async[JValue](SingleValue)
  def unwrap(): P = Parser.async[JValue](UnwrapArray)
}

object JParser {
  import scalaz.Validation.fromTryCatchNonFatal

  private def asyncParser = Parser async ValueStream

  def parseUnsafe(str: String): JValue                              = Parser.parseUnsafe[JValue](str)
  def parseFromString(str: String): Result[JValue]                  = fromTryCatchNonFatal( parseUnsafe(str) )
  def parseFromByteBuffer(buf: ByteBuffer): Result[JValue]          = Parser.parseFromByteBuffer[JValue](buf)
  def parseManyFromString(str: String): Result[Seq[JValue]]         = asyncParser absorb str
  def parseManyFromByteBuffer(buf: ByteBuffer): Result[Seq[JValue]] = asyncParser absorb buf
}

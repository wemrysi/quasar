package ygg.json

import jawn._
import jawn.AsyncParser._
import ygg.common._
import scalaz.Validation.fromTryCatchNonFatal

case class AsyncParse[A](errors: Seq[ParseException], values: Seq[A])

object AsyncParser {
  def stream(): AsyncParser[JValue] = Parser.async[JValue](ValueStream)
  def json(): AsyncParser[JValue]   = Parser.async[JValue](SingleValue)
  def unwrap(): AsyncParser[JValue] = Parser.async[JValue](UnwrapArray)
}

object JParser {
  def parseFromPath(path: String): JValue                           = macro ygg.macros.JsonMacros.parseFromPathImpl
  def parseFromStream(in: InputStream): JValue                      = parseUnsafe(slurpString(in))
  def parseFromResource[A: CTag](name: String): JValue              = parseUnsafe(slurpString(jResource[A](name)))
  def parseUnsafe(str: String): JValue                              = Parser.parseUnsafe[JValue](str)
  def parseFromString(str: String): Result[JValue]                  = fromTryCatchNonFatal( parseUnsafe(str) )
  def parseFromByteBuffer(buf: ByteBuffer): Result[JValue]          = Parser.parseFromByteBuffer[JValue](buf)
  def parseManyFromString(str: String): Result[Seq[JValue]]         = AsyncParser.stream absorb str
  def parseManyFromByteBuffer(buf: ByteBuffer): Result[Seq[JValue]] = AsyncParser.stream absorb buf
}

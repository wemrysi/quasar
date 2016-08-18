package blueeyes
package json

import scalaz._
import Validation.fromTryCatchNonFatal

object JParser {
  type Result[A] = Validation[Throwable, A]

  def parseUnsafe(str: String): JValue = new StringParser(str).parse()

  def parseFromString(str: String): Result[JValue]          = fromTryCatchNonFatal(new StringParser(str).parse())
  def parseManyFromString(str: String): Result[Seq[JValue]] = fromTryCatchNonFatal(new StringParser(str).parseMany())

  def parseFromByteBuffer(buf: ByteBuffer): Result[JValue]          = fromTryCatchNonFatal(new ByteBufferParser(buf).parse())
  def parseManyFromByteBuffer(buf: ByteBuffer): Result[Seq[JValue]] = fromTryCatchNonFatal(new ByteBufferParser(buf).parseMany())
}

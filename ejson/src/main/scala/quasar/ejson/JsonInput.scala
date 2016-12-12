/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.ejson

import quasar.Predef._
import jawn._
import java.nio.ByteBuffer.wrap
import java.nio.file.Files

/** An abstraction to simplify connecting the various forms in which
 *  raw json data might appear (strings, files, blobs) to the various
 *  types into which we might wish to deserialize it by way of a
 *  jawn Facade wrapper.
 */
sealed trait JsonInput

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object JsonInput {
  final case class ByteArray(buf: Array[scala.Byte])    extends JsonInput
  final case class ByteBuffer(buf: java.nio.ByteBuffer) extends JsonInput
  final case class File(file: java.io.File)             extends JsonInput
  final case class Path(path: java.nio.file.Path)       extends JsonInput
  final case class String(s: java.lang.String)          extends JsonInput

  implicit def fromByteArray(buf: Array[scala.Byte]): JsonInput    = ByteArray(buf)
  implicit def fromByteBuffer(buf: java.nio.ByteBuffer): JsonInput = ByteBuffer(buf)
  implicit def fromFile(file: java.io.File): JsonInput             = File(file)
  implicit def fromPath(path: java.nio.file.Path): JsonInput       = Path(path)
  implicit def fromString(s: java.lang.String): JsonInput          = String(s)

  def readOneFrom[A](p: SupportParser[A], in: JsonInput): Try[A] = in match {
    case JsonInput.ByteArray(buf)  => p parseFromByteBuffer wrap(buf)
    case JsonInput.ByteBuffer(buf) => p parseFromByteBuffer buf
    case JsonInput.File(file)      => p parseFromFile file
    case JsonInput.Path(path)      => p parseFromPath path.toString
    case JsonInput.String(s)       => p parseFromString s
  }
  def readSeqFrom[A](sp: SupportParser[A], in: JsonInput): Try[Vector[A]] = {
    import sp.facade

    def eitherToTry(x: Either[_, scSeq[A]]): Try[Vector[A]] = x match {
      case Left(t: Throwable) => scala.util.Failure(t)
      case Left(t)            => scala.util.Failure(new RuntimeException(t.toString))
      case Right(x)           => scala.util.Success(x.toVector)
    }

    val p = sp async AsyncParser.ValueStream
    val parsed = in match {
      case JsonInput.ByteArray(buf)  => p absorb buf
      case JsonInput.ByteBuffer(buf) => p absorb buf
      case JsonInput.String(s)       => p absorb s
      case JsonInput.File(file)      => p absorb Files.readAllBytes(file.toPath)
      case JsonInput.Path(path)      => p absorb Files.readAllBytes(path)
    }

    eitherToTry(
      for (r1 <- parsed.right ; r2 <- p.finish.right) yield r1 ++ r2
    )
  }
}

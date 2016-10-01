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

package ygg.json

import jawn._, jawn.AsyncParser._
import ygg._, common._
import scalaz.Validation.fromTryCatchNonFatal

final case class AsyncParse[A](errors: scSeq[ParseException], values: scSeq[A])

object AsyncParser {
  def stream(): AsyncParser[JValue] = Parser.async[JValue](ValueStream)
  def json(): AsyncParser[JValue]   = Parser.async[JValue](SingleValue)
  def unwrap(): AsyncParser[JValue] = Parser.async[JValue](UnwrapArray)
}

object JParser {
  def parseFromPath(path: String): JValue                           = parseFromPath(java.nio.file.Paths get path)
  def parseFromPath(path: jPath): JValue                            = parseUnsafe(path.slurpString)
  def parseFromStream(in: InputStream): JValue                      = parseUnsafe(slurpString(in))
  def parseFromResource[A: CTag](name: String): JValue              = parseUnsafe(slurpString(jResource[A](name)))
  def parseUnsafe(str: String): JValue                              = Parser.parseUnsafe[JValue](str)
  def parseFromString(str: String): Result[JValue]                  = fromTryCatchNonFatal( parseUnsafe(str) )
  def parseFromByteBuffer(buf: ByteBuffer): Result[JValue]          = Parser.parseFromByteBuffer[JValue](buf)
  def parseManyFromString(str: String): Result[Vec[JValue]]         = AsyncParser.stream absorb str mapRight (_.toVector)
  def parseManyFromByteBuffer(buf: ByteBuffer): Result[Vec[JValue]] = AsyncParser.stream absorb buf mapRight (_.toVector)
}

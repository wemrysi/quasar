/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.precog.common.ingest

import quasar.precog.{MimeType, MimeTypes}

import java.util.Base64

sealed trait ContentEncoding {
  def id: String
  def encode(raw: Array[Byte]): String
  def decode(compressed: String): Array[Byte]
}

object RawUTF8Encoding extends ContentEncoding {
  val id = "uncompressed"
  def encode(raw: Array[Byte])   = new String(raw, "UTF-8")
  def decode(compressed: String) = compressed.getBytes("UTF-8")
}

object Base64Encoding extends ContentEncoding {
  val id = "base64"
  def encode(raw: Array[Byte])   = new String(Base64.getEncoder.encode(raw), "UTF-8")
  def decode(compressed: String) = Base64.getDecoder.decode(compressed)
}

case class FileContent(data: Array[Byte], mimeType: MimeType, encoding: ContentEncoding)

object FileContent {
  import MimeTypes._
  val XQuirrelScript  = MimeType("text", "x-quirrel-script")
  val XJsonStream     = MimeType("application", "x-json-stream")
  val ApplicationJson = application / json
  val TextCSV         = text / csv
  val TextPlain       = text / plain
  val AnyMimeType     = MimeType("*", "*")

  val stringTypes = Set(XQuirrelScript, ApplicationJson, TextCSV, TextPlain)

  def apply(data: Array[Byte], mimeType: MimeType): FileContent =
    if (stringTypes.contains(mimeType)) {
      FileContent(data, mimeType, RawUTF8Encoding)
    } else {
      FileContent(data, mimeType, Base64Encoding)
    }
}

/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.common
package ingest

import blueeyes._, json._, serialization._
import DefaultSerialization._
import Versioned._
import Extractor._
import java.util.Base64
import scalaz._, Scalaz._
import quasar.precog._

sealed trait ContentEncoding {
  def id: String
  def encode(raw: Array[Byte]): String
  def decode(compressed: String): Array[Byte]
}

object ContentEncoding {
  val decomposerV1: Decomposer[ContentEncoding] = new Decomposer[ContentEncoding] {
    def decompose(ce: ContentEncoding) = JObject("encoding" -> ce.id.serialize)
  }

  val extractorV1: Extractor[ContentEncoding] = new Extractor[ContentEncoding] {
    override def validated(obj: JValue): Validation[Error, ContentEncoding] = {
      obj.validated[String]("encoding").flatMap {
        case "uncompressed" => Success(RawUTF8Encoding)
        case "base64"       => Success(Base64Encoding)
        case invalid        => Failure(Invalid("Unknown encoding " + invalid))
      }
    }
  }

  implicit val decomposer = decomposerV1.versioned(Some("1.0".v))
  implicit val extractor  = extractorV1.versioned(Some("1.0".v))
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
  val XQuirrelData    = MimeType("application", "x-quirrel-data")
  val XQuirrelScript  = MimeType("text", "x-quirrel-script")
  val XJsonStream     = MimeType("application", "x-json-stream")
  val ApplicationJson = application / json
  val TextCSV         = text / csv
  val TextPlain       = text / plain
  val AnyMimeType     = MimeType("*", "*")
  val OctetStream     = application / `octet-stream`
  val stringTypes     = Set(XQuirrelScript, ApplicationJson, TextCSV, TextPlain)

  def apply(data: Array[Byte], mimeType: MimeType): FileContent =
    if (stringTypes.contains(mimeType)) {
      FileContent(data, mimeType, RawUTF8Encoding)
    } else {
      FileContent(data, mimeType, Base64Encoding)
    }
}

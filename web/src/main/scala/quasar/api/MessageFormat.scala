/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.api

import argonaut.EncodeJson
import org.http4s.parser.HttpHeaderParser
import quasar.Predef._
import quasar.fp._

import org.http4s.{DecodeResult, EntityDecoder, MediaType}
import org.http4s.headers.{Accept, `Content-Disposition`}
import quasar.fs.WriteError
import quasar.repl.Prettify

import quasar.{DataEncodingError, Data, DataCodec}

import scalaz.NonEmptyList
import scalaz.syntax.applicative._
import scalaz.std.option._
import scalaz.stream.Process

sealed trait JsonPrecision {
  def codec: DataCodec
  def name: String
}
object JsonPrecision {
  case object Readable extends JsonPrecision {
    val codec = DataCodec.Readable
    val name = "readable"
  }
  case object Precise extends JsonPrecision {
    val codec = DataCodec.Precise
    val name = "precise"
  }
}
sealed trait JsonFormat {
  def mediaType: MediaType
}
object JsonFormat {
  case object LineDelimited extends JsonFormat {
    val mediaType = new MediaType("application", "ldjson", true, true) // ldjson => line delimited json
  }
  case object SingleArray extends JsonFormat {
    val mediaType = MediaType.`application/json`
  }
}

sealed trait MessageFormat {
  def mediaType: MediaType
  def disposition: Option[`Content-Disposition`]
  def encode[F[_]](data: Process[F, Data]): Process[F, String]
  def decode(txt: String): (List[WriteError], List[Data])
  def decoder: EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(mediaType) { msg =>
    DecodeResult.success(EntityDecoder.decodeString(msg).map(decode))
  }
  protected def dispositionExtension: Map[String, String] =
    disposition.map(disp => Map("disposition" -> disp.value)).getOrElse(Map.empty)
}
object MessageFormat {
  final case class JsonContentType(
    mode: JsonPrecision,
    format: JsonFormat,
    disposition: Option[`Content-Disposition`]
  ) extends MessageFormat {
    val LineSep = "\r\n"
    def mediaType = {
      val extensions = Map("mode" -> mode.name) ++ dispositionExtension
      format.mediaType.withExtensions(extensions)
    }
    override def encode[F[_]](data: Process[F, Data]): Process[F, String] = {
      val encodedData =
        data.map(DataCodec.render(_)(mode.codec).fold(EncodeJson.of[DataEncodingError].encode(_).toString, ι))
      format match {
        case JsonFormat.SingleArray =>
          Process.emit("[" + LineSep) ++ encodedData.intersperse("," + LineSep) ++ Process.emit(LineSep + "]" + LineSep)
        case JsonFormat.LineDelimited =>
          encodedData.map(_ + LineSep)
      }
    }
    override def decode(txt: String): (List[WriteError], List[Data]) = {
      implicit val codec = mode.codec
      format match {
        case JsonFormat.SingleArray =>
          DataCodec.parse(txt).fold(
            err => (List(WriteError(Data.Str("parse error: " + err.message), None)), List()),
            data => (List(),List(data))
          )
        case JsonFormat.LineDelimited =>
          unzipDisj(
            txt.split("\n").map(line => DataCodec.parse(line).leftMap(
              e => WriteError(Data.Str("parse error: " + line), Some(e.message))
            )).toList
          )
      }
    }
  }

  object JsonContentType {
    def apply(mode: JsonPrecision, format: JsonFormat): JsonContentType = JsonContentType(mode, format, disposition = None)
  }

  val Default = JsonContentType(JsonPrecision.Readable, JsonFormat.LineDelimited, None)

  final case class Csv(columnDelimiter: Char, rowDelimiter: String, quoteChar: Char, escapeChar: Char, disposition: Option[`Content-Disposition`]) extends MessageFormat {
    import Csv._

    val CsvColumnsFromInitialRowsCount = 1000

    def mediaType = {
      val alwaysExtensions = Map(
        "columnDelimiter" -> escapeNewlines(columnDelimiter.toString),
        "rowDelimiter" -> escapeNewlines(rowDelimiter),
        "quoteChar" -> escapeNewlines(quoteChar.toString),
        "escapeChar" -> escapeNewlines(escapeChar.toString))
      val extensions = alwaysExtensions ++ dispositionExtension
      Csv.mediaType.withExtensions(extensions)
    }
    override def encode[F[_]](data: Process[F, Data]): Process[F, String] = {
      import quasar.repl.Prettify
      import com.github.tototoshi.csv._

      val format = Some(CsvParser.Format(columnDelimiter,quoteChar,escapeChar,rowDelimiter))

      Prettify.renderStream(data, CsvColumnsFromInitialRowsCount).map { v =>
        val w = new java.io.StringWriter
        val cw = format.map(f => CSVWriter.open(w)(f)).getOrElse(CSVWriter.open(w))
        cw.writeRow(v)
        cw.close
        w.toString
      }
    }
    override def decode(txt: String): (List[WriteError], List[Data]) = Csv.decode(txt)
  }
  object Csv {
    val mediaType = MediaType.`text/csv`

    val Default = Csv(',', "\r\n", '"', '"', None)

    def escapeNewlines(str: String): String =
      str.replace("\r", "\\r").replace("\n", "\\n")

    def unescapeNewlines(str: String): String =
      str.replace("\\r", "\r").replace("\\n", "\n")

    def decode(txt: String): (List[WriteError], List[Data]) = {
      CsvDetect.parse(txt).fold(
        err => List(WriteError(Data.Str("parse error: " + err), None)) -> Nil,
        lines => lines.headOption.map { header =>
          val paths = header.fold(κ(Nil), _.fields.map(Prettify.Path.parse(_).toOption))
          val rows = lines.drop(1).map(_.bimap(
            err => WriteError(Data.Str("parse error: " + err), None),
            rec => {
              val pairs = (paths zip rec.fields.map(Prettify.parse))
              val good = pairs.map { case (p, s) => (p |@| s).tupled }.flatten
              Prettify.unflatten(good.toListMap)
            }
          )).toList
          unzipDisj(rows)
        }.getOrElse(List(WriteError(Data.Obj(ListMap()), Some("no CSV header in body"))) -> Nil))
    }
    def decoder: EntityDecoder[(List[WriteError], List[Data])] = EntityDecoder.decodeBy(mediaType) { msg =>
      DecodeResult.success(EntityDecoder.decodeString(msg).map(decode))
    }
  }

  case object UnsupportedContentType extends scala.Exception

  val decoder: EntityDecoder[(List[WriteError], List[Data])] = {
    // TODO: Reformat this
    import JsonPrecision._
    import JsonFormat._
    Csv.decoder orElse
      JsonContentType(Readable,LineDelimited).decoder orElse
      JsonContentType(Precise,LineDelimited).decoder orElse
      JsonContentType(Readable,SingleArray).decoder orElse
      JsonContentType(Precise,SingleArray).decoder
  }

  def fromAccept(accept: Option[Accept]): MessageFormat = {
    val mediaTypes = NonEmptyList(
      JsonFormat.LineDelimited.mediaType,
      new MediaType("application", "x-ldjson"),
      JsonFormat.SingleArray.mediaType,
      Csv.mediaType)

    (for {
      acc       <- accept
      // TODO: MediaRange needs an Order instance – combining QValue ordering
      //       with specificity (EG, application/json sorts before
      //       application/* if they have the same q-value).
      chosenMediaType <- acc.values.sortBy(_.qValue).list.find(a => mediaTypes.list.exists(a.satisfies(_)))
    } yield {
      val disposition = chosenMediaType.extensions.get("disposition").flatMap { str =>
        HttpHeaderParser.CONTENT_DISPOSITION(str).toOption
      }
      if (chosenMediaType satisfies Csv.mediaType) {
        def toChar(str: String): Option[Char] = str.toList match {
          case c :: Nil => Some(c)
          case _ => None
        }
        Csv(chosenMediaType.extensions.get("columnDelimiter").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse(','),
          chosenMediaType.extensions.get("rowDelimiter").map(Csv.unescapeNewlines).getOrElse("\r\n"),
          chosenMediaType.extensions.get("quoteChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          chosenMediaType.extensions.get("escapeChar").map(Csv.unescapeNewlines).flatMap(toChar).getOrElse('"'),
          disposition)
      }
      else {
        val format = if ((chosenMediaType satisfies JsonFormat.SingleArray.mediaType) &&
                          chosenMediaType.extensions.get("boundary") != Some("NL")) JsonFormat.SingleArray
                     else JsonFormat.LineDelimited
        val precision =
          if (chosenMediaType.extensions.get("mode") == Some(JsonPrecision.Precise.name))
            JsonPrecision.Precise
          else
            JsonPrecision.Readable
         JsonContentType(precision, format, disposition)
      }
    }).getOrElse(MessageFormat.Default)
  }
}

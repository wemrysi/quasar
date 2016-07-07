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

import blueeyes.json._
import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.json.AsyncParser._

import scalaz._
import scalaz.syntax.monad._

trait JValueByteChunkTranscoders {
  private implicit val seqJValueMonoid = new Monoid[Seq[JValue]] {
    def zero = Seq.empty[JValue]
    def append(xs: Seq[JValue], ys: => Seq[JValue]) = xs ++ ys
  }

  implicit def JValueByteChunkTranscoder(implicit M: Monad[Future]) = new AsyncHttpTranscoder[JValue, ByteChunk] {
    def apply(req: HttpRequest[JValue]): HttpRequest[ByteChunk] =
      req.copy(content = req.content.map { (j: JValue) =>
        Left(j.renderCompact.getBytes("UTF-8"))
      })

    def unapply(fres: Future[HttpResponse[ByteChunk]]): Future[HttpResponse[JValue]] = {
      fres.flatMap { res =>
        res.content match {
          case Some(bc) =>
            val fv: Future[Validation[Seq[Throwable], JValue]] =
              JsonUtil.parseSingleFromByteChunk(bc)
            fv.map(v => res.copy(content = v.toOption))
          case None =>
            M.point(res.copy(content = None))
        }
      }
    }
  }
}

object JValueByteChunkTranscoders extends JValueByteChunkTranscoders

object JsonUtil {
  def parseSingleFromByteChunk(bc: ByteChunk)(implicit M: Monad[Future]): Future[Validation[Seq[Throwable], JValue]] =
    parseSingleFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseSingleFromStream[M[+_]: Monad](stream: StreamT[M, Array[Byte]]): M[Validation[Seq[Throwable], JValue]] = {
    def rec(stream: StreamT[M, Array[Byte]], parser: AsyncParser): M[Validation[Seq[Throwable], JValue]] = {
      def handle(ap: AsyncParse, next: => M[Validation[Seq[Throwable], JValue]]): M[Validation[Seq[Throwable], JValue]] = ap match {
        case AsyncParse(errors, _) if errors.nonEmpty =>
          Failure(errors).point[M]
        case AsyncParse(_, values) if values.nonEmpty =>
          Success(values.head).point[M]
        case _ =>
          next
      }

      stream.uncons flatMap {
        case Some((bytes, tail)) =>
          val (ap, p2) = parser(More(ByteBufferWrap(bytes)))
          handle(ap, rec(tail, p2))
        case None =>
          val (ap, p2) = parser(Done)
          handle(ap, Failure(Seq(new Exception("parse failure"))).point[M])
      }
    }

    rec(stream, AsyncParser.json())
  }


  def parseManyFromByteChunk(bc: ByteChunk)(implicit M: Monad[Future]): StreamT[Future, AsyncParse] =
    parseManyFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseManyFromStream[M[+_]: Monad](stream: StreamT[M, Array[Byte]]): StreamT[M, AsyncParse] = {
    // create a new stream, using the current stream and parser
    StreamT.unfoldM((stream, AsyncParser.stream())) {
      case (stream, parser) => stream.uncons map {
        case Some((bytes, tail)) =>
          // parse the current byte buffer, keeping track of the
          // new parser instance we were given back
          val (r, parser2) = parser(More(ByteBufferWrap(bytes)))
          Some((r, (tail, parser2)))
        case None =>
          // once we're out of byte buffers, send None to signal EOF
          val (r, parser2) = parser(Done)
          Some((r, (StreamT.empty, parser2)))
      }
    }
  }
}

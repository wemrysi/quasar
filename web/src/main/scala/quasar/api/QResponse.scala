/*
 * Copyright 2014–2017 SlamData Inc.
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

import slamdata.Predef._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.Failure
import quasar.fp._

import argonaut._, Argonaut._
import monocle._, macros.Lenses
import org.http4s._
import org.http4s.headers._
import org.http4s.dsl._
import scalaz.{Optional => _, _}
import scalaz.syntax.monad._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scodec.bits.ByteVector

@Lenses
final case class QResponse[S[_]](status: Status, headers: Headers, body: Process[Free[S, ?], ByteVector]) {
  import QResponse.HttpResponseStreamFailureException

  def flatMapS[T[_]](f: S ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.flatMapSNT(f)))

  def mapS[T[_]](f: S ~> T): QResponse[T] =
    copy[T](body = body.translate[Free[T, ?]](free.mapSNT(f)))

  def translate[T[_]](f: Free[S, ?] ~> Free[T, ?]): QResponse[T] =
    copy[T](body = body.translate(f))

  def modifyHeaders(f: Headers => Headers): QResponse[S] =
    QResponse.headers.modify(f)(this)

  def toHttpResponse(i: S ~> ResponseOr): Response =
    toHttpResponseF(free.foldMapNT(i))

  def toHttpResponseF(i: Free[S, ?] ~> ResponseOr): Response = {
    val failTask: ResponseOr ~> Task = new (ResponseOr ~> Task) {
      def apply[A](ror: ResponseOr[A]) =
        ror.fold(resp => Task.fail(new HttpResponseStreamFailureException(resp)), _.point[Task]).join
    }

    def handleBytes(bytes: Process[ResponseOr, ByteVector]): Response =
      Response(body = bytes.translate(failTask))
        .withStatus(status)
        .putHeaders(headers.toList: _*)

    handleBytes(body.translate[ResponseOr](i))
  }

  def withStatus(s: Status): QResponse[S] =
    QResponse.status.set(s)(this)
}

object QResponse {
  final class HttpResponseStreamFailureException(alternate: Response)
    extends java.lang.Exception

  def empty[S[_]]: QResponse[S] =
    QResponse(NoContent, Headers.empty, Process.halt)

  def ok[S[_]]: QResponse[S] =
    empty[S].withStatus(Ok)

  def header[S[_]](key: HeaderKey.Extractable): Optional[QResponse[S], key.HeaderT] =
    Optional[QResponse[S], key.HeaderT](
      qr => qr.headers.get(key))(
      h  => _.modifyHeaders(_.put(h)))

  def json[A: EncodeJson, S[_]](status: Status, a: A): QResponse[S] =
    string[S](status, a.asJson.pretty(minspace)).modifyHeaders(_.put(
      `Content-Type`(MediaType.`application/json`, Some(Charset.`UTF-8`))))

  def response[S[_], A]
      (status: Status, a: A)
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QResponse[S] =
    QResponse(
      status,
      E.headers,
      Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](free.injectFT))

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  def streaming[S[_], A]
      (p: Process[Free[S, ?], A])
      (implicit E: EntityEncoder[A], S0: Task :<: S)
      : QResponse[S] =
    QResponse(
      Ok,
      E.headers,
      p.flatMap[Free[S, ?], ByteVector](a =>
        Process.await(E.toEntity(a))(_.body).translate[Free[S, ?]](free.injectFT)))

  def streaming[S[_], A, E]
      (p: Process[EitherT[Free[S, ?], E, ?], A])
      (implicit A: EntityEncoder[A], S0: Task :<: S, S1: Failure[E, ?] :<: S)
      : QResponse[S] = {
    val failure = Failure.Ops[E, S]
    streaming(p.translate(failure.unattemptT))
  }

  def string[S[_]](status: Status, s: String): QResponse[S] =
    QResponse(
      status,
      Headers(`Content-Type`(MediaType.`text/plain`, Some(Charset.`UTF-8`))),
      Process.emit(ByteVector.view(s.getBytes(Charset.`UTF-8`.nioCharset))))
}

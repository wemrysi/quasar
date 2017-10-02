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

import slamdata.Predef.PartialFunction
import quasar.fp._
import quasar.fp.free
import quasar.fp.ski._
import org.http4s.{Request, Status, HttpService}
import scalaz.std.iterable._
import scalaz._, Scalaz._

final class QHttpService[S[_]] private (val f: PartialFunction[Request, Free[S, QResponse[S]]]) {
  def apply(req: Request): Free[S, QResponse[S]] =
    f.applyOrElse(req, κ(Free.pure(QResponse.empty[S].withStatus(Status.NotFound))))

  def flatMapS[T[_]](g: S ~> Free[T, ?]): QHttpService[T] =
    new QHttpService(f.andThen(_.map(_.flatMapS(g)).flatMapSuspension(g)))

  def mapS[T[_]](g: S ~> T): QHttpService[T] =
    new QHttpService(f.andThen(_.map(_.mapS(g)).mapSuspension(g)))

  def translate[T[_]](g: Free[S, ?] ~> Free[T, ?]): QHttpService[T] =
    new QHttpService(f.andThen(resp => g(resp).map(_.translate(g))))

  def orElse(other: QHttpService[S]): QHttpService[S] =
    new QHttpService(f orElse other.f)

  def toHttpService(i: S ~> ResponseOr): HttpService =
    toHttpServiceF(free.foldMapNT(i))

  def toHttpServiceF(i: Free[S, ?] ~> ResponseOr): HttpService = {
    def mkResponse(prg: Free[S, QResponse[S]]) =
      i(prg).map(_.toHttpResponseF(i)).merge

    HttpService(f andThen mkResponse)
  }
}

object QHttpService {
  /** Producing this many bytes from a `Process[F, ByteVector]` should require
    * at least one `F` effect.
    *
    * The scenarios this is intended to handle involve prepending a small wrapper
    * around a stream, like "[\n" when outputting JSON data in an array, and thus
    * 100 bytes seemed large enough to contain these cases and small enough as to
    * not force more than is needed.
    *
    * This exists because of how http4s handles errors from `Process` responses.
    * If an error is produced by a `Process` while streaming the connection is
    * severed, but the headers and status code have already been emitted to the
    * wire so it isn't possible to emit a useful error message or status. In an
    * attempt to handle many common scenarios where the first effect in the stream
    * is the most likely to error (i.e. opening a file, or other resource to stream
    * from) we'd like to step the stream until we've reached the first `F` effect
    * so that we can see if it succeeds before continuing with the rest of the
    * stream, providing a chance to respond with an error in the failure case.
    *
    * We cannot just `Process.unemit` the `Process` as there may be non-`F` `Await`
    * steps encountered before an actual `F` effect (from many of the combinators
    * in `process1` and the like).
    *
    * This leads us to the current workaround which is to define this threshold
    * which should be, ideally, just large enough to require the first `F` to
    * produce the bytes, but not more. We then consume the byte stream until it
    * ends or we've consumed this many bytes. Finally we have a chance to inspect
    * the `F` and see if anything failed before handing the rest of the process
    * to http4s to continue streaming to the client as normal.
    */
  val PROCESS_EFFECT_THRESHOLD_BYTES = 100L

  def apply[S[_]](
    f: PartialFunction[Request, Free[S, QResponse[S]]]
  )(implicit
    C: Catchable[Free[S, ?]]
  ): QHttpService[S] =
    new QHttpService(
      f andThen (_ >>= (r =>
        r.body
          .stepUntil(_.foldMap(_.length) >= PROCESS_EFFECT_THRESHOLD_BYTES)
          .map(b => r.copy(body = b)))))
}

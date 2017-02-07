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

package quasar.contrib.scalaz

import java.lang.{Throwable, RuntimeException}

import scalaz._, Scalaz._, Leibniz.===

final class CatchableOfDisjunctionOps[F[_], A, B] private[scalaz] (self: F[A \/ B])(implicit F: Catchable[F]) {
  def unattempt(implicit M: Monad[F], T: A === Throwable): F[B] =
    self flatMap (_.fold(a => F.fail[B](T(a)), M.point(_)))

  def unattemptRuntime(implicit M: Monad[F], S: Show[A]): F[B] =
    new CatchableOfDisjunctionOps(
      self map (_.leftMap[Throwable](a => new RuntimeException(a.shows)))
    ).unattempt
}

trait ToCatchableOps {
  implicit def toCatchableOfDisjunctionOps[F[_]: Catchable, A, B](self: F[A \/ B]): CatchableOfDisjunctionOps[F, A, B] =
    new CatchableOfDisjunctionOps(self)
}

object catchable extends ToCatchableOps

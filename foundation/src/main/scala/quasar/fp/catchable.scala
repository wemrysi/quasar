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

package quasar.fp

import java.lang.RuntimeException
import scalaz._, Scalaz._

trait CatchableInstances {
  implicit class CatchableOfDisjunctionOps[F[_]: Catchable, A, B](self: F[A \/ B]) {
    def unattempt(implicit ev0: Monad[F], ev1: Show[A]): F[B] =
      self.flatMap(_.fold(a => Catchable[F].fail(new RuntimeException(a.shows)), _.point[F]))
  }
}

object catchable extends CatchableInstances

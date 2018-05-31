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

package quasar.contrib.fs2

import slamdata.Predef.{None, Option, Some}

import java.util.Iterator
import java.util.stream.{Stream => JStream}

import fs2.Stream
import fs2.interop.scalaz._
import fs2.util.Suspendable
import scalaz.syntax.monad._

object jstream {
  def asStream[F[_], A](js: F[JStream[A]])(implicit F: Suspendable[F]): Stream[F, A] = {
    def getNext(i: Iterator[A]): F[Option[(A, Iterator[A])]] =
      F.delay(i.hasNext).ifM(
        F.delay(Some((i.next(), i))),
        F.pure(None))

    Stream.bracket(js)(
      s => Stream.unfoldEval(s.iterator)(getNext),
      s => F.delay(s.close()))
  }
}

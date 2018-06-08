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

package quasar.yggdrasil

import quasar.contrib.scalaz.catchable
import quasar.contrib.iota.{:<<:, ACopK}

import fs2.util.Catchable

import scalaz.Free
import scalaz.concurrent.Task
import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

import scala.util.Either

package object vfs {
  type POSIX[A] = Free[POSIXOp, A]
  type POSIXWithTaskCopK[A] = CopK[POSIXOp ::: Task ::: TNilK, A]
  type POSIXWithTask[A] = Free[POSIXWithTaskCopK, A]

  // this is needed kind of a lot
  private[vfs] implicit def catchableForS[S[a] <: ACopK[a]](implicit I: Task :<<: S): Catchable[Free[S, ?]] = {
    val delegate = catchable.copKinjectableTaskCatchable[S]

    new Catchable[Free[S, ?]] {

      def pure[A](a: A): Free[S, A] =
        Free.pure[S, A](a)

      def attempt[A](fa: Free[S, A]): Free[S, Either[Throwable, A]] =
        delegate.attempt(fa).map(_.toEither)

      def fail[A](err: Throwable): Free[S, A] =
        delegate.fail(err)

      def flatMap[A, B](fa: Free[S, A])(f: A => Free[S, B]): Free[S, B] =
        fa.flatMap(f)
    }
  }


}

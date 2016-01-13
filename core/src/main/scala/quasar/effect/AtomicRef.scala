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

package quasar.effect

import quasar.Predef._
import quasar.SKI._
import quasar.fp.TaskRef

import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.applicative._
import scalaz.syntax.equal._
import scalaz.syntax.id._

/** A reference to a value that may be updated atomically.
  *
  * @tparam V the type of value referenced
  */
sealed trait AtomicRef[V, A]

object AtomicRef {
  /** NB: Attempted to define this as `Get[V]() extends AtomicRef[V, V]` but
    *     when pattern matching `(x: AtomicRef[A, B]) match { case Get() => }`
    *     scalac doesn't recognize that A =:= B.
    */
  final case class Get[V, A](f: V => A) extends AtomicRef[V, A]
  final case class Set[V](v: V) extends AtomicRef[V, Unit]
  final case class CompareAndSet[V](expect: V, update: V) extends AtomicRef[V, Boolean]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[V, S[_]: Functor](implicit S: AtomicRefF[V, ?] :<: S)
    extends LiftedOps[AtomicRef[V, ?], S] {

    /** Set the value of the ref to `update` if the current value == `expect`,
      * returns whether the value was updated.
      */
    def compareAndSet(expect: V, update: V): F[Boolean] =
      lift(CompareAndSet(expect, update))

    /** Returns the current value of the ref. */
    def get: F[V] =
      lift(Get(ι))

    /** Atomically updates the ref with the result of applying the given
      * function to the current value, returning the updated value.
      */
    def modify(f: V => V): F[V] =
      modifyS(v => f(v).squared)

    /** Atomically updates the ref with the first part of the result of applying
      * the given function to the current value, returning the second part.
      */
    def modifyS[A](f: V => (V, A)): F[A] =
      for {
        cur       <- get
        (nxt, a0) =  f(cur)
        updated   <- compareAndSet(cur, nxt)
        a         <- if (updated) a0.point[F] else modifyS(f)
      } yield a

    /** Sets the value of the ref to the given value. */
    def set(value: V): F[Unit] =
      lift(Set(value))
  }

  object Ops {
    def apply[V, S[_]: Functor](implicit S: AtomicRefF[V, ?] :<: S): Ops[V, S] =
      new Ops[V, S]
  }

  def fromTaskRef[A](tr: TaskRef[A]): AtomicRef[A, ?] ~> Task =
    new (AtomicRef[A, ?] ~> Task) {
      def apply[B](fb: AtomicRef[A, B]) = fb match {
        case Get(f) =>
          tr.read map f

        case Set(a) =>
          tr.write(a)

        case CompareAndSet(expect, update) =>
          tr.compareAndSet(expect, update)
      }
    }

  def toState[F[_]: Applicative, S: Equal]: AtomicRef[S, ?] ~> StateT[F, S, ?] =
    new (AtomicRef[S, ?] ~> StateT[F, S, ?]) {
      def apply[A](fa: AtomicRef[S, A]) = fa match {
        case Get(f) =>
          state(s => (s, f(s)))

        case Set(s) =>
          state(κ((s, ())))

        case CompareAndSet(expect, update) =>
          state(s => if (s === expect) (update, true) else (s, false))
      }

      def state[A](f: S => (S, A)) =
        StateT[F, S, A](s => f(s).point[F])
    }
}

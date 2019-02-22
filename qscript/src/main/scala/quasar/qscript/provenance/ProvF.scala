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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.fp.ski.κ

import matryoshka._
import monocle.Prism
import scalaz._, Scalaz._

/**
  * @tparam D type of data
  * @tparam I type of identity
  * @tparam S sorts of identities
  * @tparam A recursive position
  */
sealed abstract class ProvF[D, I, S, A]

object ProvF extends ProvFInstances {
  final case class Fresh[D, I, S, A]() extends ProvF[D, I, S, A]
  final case class PrjPath[D, I, S, A](segment: D) extends ProvF[D, I, S, A]
  final case class PrjValue[D, I, S, A](field: D, sort: S) extends ProvF[D, I, S, A]
  final case class InjValue[D, I, S, A](field: D, sort: S) extends ProvF[D, I, S, A]
  final case class Value[D, I, S, A](id: I, sort: S) extends ProvF[D, I, S, A]
  final case class Both[D, I, S, A](left: A, right: A) extends ProvF[D, I, S, A]
  final case class Then[D, I, S, A](fst: A, snd: A) extends ProvF[D, I, S, A]

  final class Optics[D, I, S] {
    def fresh[A]: Prism[ProvF[D, I, S, A], Unit] =
      Prism.partial[ProvF[D, I, S, A], Unit] {
        case Fresh() => ()
      } (κ(Fresh()))

    def prjPath[A]: Prism[ProvF[D, I, S, A], D] =
      Prism.partial[ProvF[D, I, S, A], D] {
        case PrjPath(c) => c
      } (PrjPath(_))

    def prjValue[A]: Prism[ProvF[D, I, S, A], (D, S)] =
      Prism.partial[ProvF[D, I, S, A], D] {
        case PrjValue(k, s) => (k, s)
      } {
        case (k, s) => PrjValue(k, s)
      }

    def injValue[A]: Prism[ProvF[D, I, S, A], (D, S)] =
      Prism.partial[ProvF[D, I, S, A], (D, S)] {
        case InjValue(k, s) => (k, s)
      } {
        case (k, s) => InjValue(k, s)
      }

    def value[A]: Prism[ProvF[D, I, S, A], (I, S)] =
      Prism.partial[ProvF[D, I, S, A], (I, S)] {
        case Value(i, s) => (i, s)
      } {
        case (i, s) => Value(i, s)
      }

    def both[A]: Prism[ProvF[D, I, S, A], (A, A)] =
      Prism.partial[ProvF[D, I, S, A], (A, A)] {
        case Both(l, r) => (l, r)
      } {
        case (l, r) => Both(l, r)
      }

    // NB: 'then' is now a reserved identifier in Scala
    def thenn[A]: Prism[ProvF[D, I, S, A], (A, A)] =
      Prism.partial[ProvF[D, I, S, A], (A, A)] {
        case Then(f, s) => (f, s)
      } {
        case (f, s) => Then(f, s)
      }
  }

  object Optics {
    def apply[D, I, S]: Optics[D, I, S] = new Optics[D, I, S]
  }
}

sealed abstract class ProvFInstances {
  import ProvF._

  implicit def traverse[D, I, S]: Traverse[ProvF[D, I, S, ?]] =
    new Traverse[ProvF[D, I, S, ?]] {
      val O = Optics[D, I, S]

      def traverseImpl[F[_]: Applicative, A, B](p: ProvF[D, I, S, A])(g: A => F[B]) =
        p match {
          case Fresh() => O.fresh[B]().point[F]
          case PrjPath(d) => O.prjPath[B](d).point[F]
          case PrjValue(d, s) => O.prjValue[B](d, s).point[F]
          case InjValue(d, s) => O.injValue[B](d, s).point[F]
          case Value(i, s) => O.value[B](i, s).point[F]
          case Both(l, r) => g(l).tuple(g(r)).map(O.both(_))
          case Then(f, s) => g(f).tuple(g(s)).map(O.thenn(_))
        }
    }

  implicit def show[D: Show, I: Show, S: Show]: Delay[Show, ProvF[D, I, S, ?]] =
    new Delay[Show, ProvF[D, I, S, ?]] {
      def apply[A](show: Show[A]) = {
        implicit val showA = show
        Show.show {
          case Fresh() => Cord("∃")
          case PrjPath(d) => Cord("\\") ++ d.show
          case PrjValue(d, s) => Cord("prj{") ++ d.show ++ Cord("} :: ") ++ s.show
          case InjValue(d, s) => Cord("inj{") ++ d.show ++ Cord("} :: ") ++ s.show
          case Value(i, s) => i.show ++ Cord(" :: ") ++ s.show
          case Both(l, r) => Cord("(") ++ l.show ++ Cord(" ∧ ") ++ r.show ++ Cord(")")
          case Then(f, s) => f.show ++ Cord(" ≺ ") ++ s.show
        }
      }
    }
}

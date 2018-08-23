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
  * @tparam A recursive position
  */
sealed abstract class ProvF[D, I, A]

object ProvF extends ProvFInstances {
  final case class Fresh[D, I, A]() extends ProvF[D, I, A]
  final case class PrjPath[D, I, A](segment: D) extends ProvF[D, I, A]
  final case class PrjValue[D, I, A](field: D) extends ProvF[D, I, A]
  final case class InjValue[D, I, A](field: D) extends ProvF[D, I, A]
  final case class Value[D, I, A](id: I) extends ProvF[D, I, A]
  final case class Both[D, I, A](left: A, right: A) extends ProvF[D, I, A]
  final case class Then[D, I, A](fst: A, snd: A) extends ProvF[D, I, A]

  final class Optics[D, I] {
    def fresh[A]: Prism[ProvF[D, I, A], Unit] =
      Prism.partial[ProvF[D, I, A], Unit] {
        case Fresh() => ()
      } (κ(Fresh()))

    def prjPath[A]: Prism[ProvF[D, I, A], D] =
      Prism.partial[ProvF[D, I, A], D] {
        case PrjPath(c) => c
      } (PrjPath(_))

    def prjValue[A]: Prism[ProvF[D, I, A], D] =
      Prism.partial[ProvF[D, I, A], D] {
        case PrjValue(k) => k
      } (PrjValue(_))

    def injValue[A]: Prism[ProvF[D, I, A], D] =
      Prism.partial[ProvF[D, I, A], D] {
        case InjValue(k) => k
      } (InjValue(_))

    def value[A]: Prism[ProvF[D, I, A], I] =
      Prism.partial[ProvF[D, I, A], I] {
        case Value(i) => i
      } (Value(_))

    def both[A]: Prism[ProvF[D, I, A], (A, A)] =
      Prism.partial[ProvF[D, I, A], (A, A)] {
        case Both(l, r) => (l, r)
      } {
        case (l, r) => Both(l, r)
      }

    // NB: 'then' is now a reserved identifier in Scala
    def thenn[A]: Prism[ProvF[D, I, A], (A, A)] =
      Prism.partial[ProvF[D, I, A], (A, A)] {
        case Then(f, s) => (f, s)
      } {
        case (f, s) => Then(f, s)
      }
  }

  object Optics {
    def apply[D, I]: Optics[D, I] = new Optics[D, I]
  }
}

sealed abstract class ProvFInstances {
  import ProvF._

  implicit def traverse[D, I]: Traverse[ProvF[D, I, ?]] =
    new Traverse[ProvF[D, I, ?]] {
      val O = Optics[D, I]

      def traverseImpl[F[_]: Applicative, A, B](p: ProvF[D, I, A])(g: A => F[B]) =
        p match {
          case Fresh() => O.fresh[B]().point[F]
          case PrjPath(d) => O.prjPath[B](d).point[F]
          case PrjValue(d) => O.prjValue[B](d).point[F]
          case InjValue(d) => O.injValue[B](d).point[F]
          case Value(i) => O.value[B](i).point[F]
          case Both(l, r) => g(l).tuple(g(r)).map(O.both(_))
          case Then(f, s) => g(f).tuple(g(s)).map(O.thenn(_))
        }
    }

  implicit def show[D: Show, I: Show]: Delay[Show, ProvF[D, I, ?]] =
    new Delay[Show, ProvF[D, I, ?]] {
      def apply[A](show: Show[A]) = {
        implicit val showA = show
        Show.show {
          case Fresh() => Cord("∃")
          case PrjPath(d) => Cord("\\") ++ d.show
          case PrjValue(d) => Cord("prj{") ++ d.show ++ Cord("}")
          case InjValue(d) => Cord("inj{") ++ d.show ++ Cord("}")
          case Value(i) => i.show
          case Both(l, r) => Cord("(") ++ l.show ++ Cord(" ∧ ") ++ r.show ++ Cord(")")
          case Then(f, s) => f.show ++ Cord(" ≺ ") ++ s.show
        }
      }
    }
}

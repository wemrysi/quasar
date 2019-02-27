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
  * @tparam D type of scalar identities
  * @tparam I type of vector identities
  * @tparam S sorts of identities
  * @tparam A recursive position
  */
sealed abstract class ProvF[D, I, S, A]

object ProvF extends ProvFInstances {
  final case class Fresh[D, I, S, A]() extends ProvF[D, I, S, A]
  final case class Project[D, I, S, A](id: D, sort: S) extends ProvF[D, I, S, A]
  final case class Inject[D, I, S, A](id: D, sort: S) extends ProvF[D, I, S, A]
  final case class Inflate[D, I, S, A](id: I, sort: S) extends ProvF[D, I, S, A]
  final case class Both[D, I, S, A](left: A, right: A) extends ProvF[D, I, S, A]
  final case class Then[D, I, S, A](fst: A, snd: A) extends ProvF[D, I, S, A]

  final class Optics[D, I, S] {
    def fresh[A]: Prism[ProvF[D, I, S, A], Unit] =
      Prism.partial[ProvF[D, I, S, A], Unit] {
        case Fresh() => ()
      } (κ(Fresh()))

    def project[A]: Prism[ProvF[D, I, S, A], (D, S)] =
      Prism.partial[ProvF[D, I, S, A], (D, S)] {
        case Project(d, s) => (d, s)
      } {
        case (d, s) => Project(d, s)
      }

    def inject[A]: Prism[ProvF[D, I, S, A], (D, S)] =
      Prism.partial[ProvF[D, I, S, A], (D, S)] {
        case Inject(d, s) => (d, s)
      } {
        case (d, s) => Inject(d, s)
      }

    def inflate[A]: Prism[ProvF[D, I, S, A], (I, S)] =
      Prism.partial[ProvF[D, I, S, A], (I, S)] {
        case Inflate(i, s) => (i, s)
      } {
        case (i, s) => Inflate(i, s)
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

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def traverse[D, I, S]: Traverse[ProvF[D, I, S, ?]] =
    new Traverse[ProvF[D, I, S, ?]] {
      val O = Optics[D, I, S]

      def traverseImpl[F[_]: Applicative, A, B](p: ProvF[D, I, S, A])(g: A => F[B]) =
        p match {
          case p@Fresh() => p.asInstanceOf[ProvF[D, I, S, B]].point[F]
          case Project(d, s) => O.project[B](d, s).point[F]
          case Inject(d, s) => O.inject[B](d, s).point[F]
          case Inflate(i, s) => O.inflate[B](i, s).point[F]
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
          case Project(d, s) => Cord("∏(") ++ d.show ++ Cord(" :: ") ++ s.show ++ Cord(")")
          case Inject(d, s) => Cord("I(") ++ d.show ++ Cord(" :: ") ++ s.show ++ Cord(")")
          case Inflate(i, s) => Cord("∆(") ++ i.show ++ Cord(" :: ") ++ s.show ++ Cord(")")
          case Both(l, r) => Cord("{") ++ l.show ++ Cord(" ∧ ") ++ r.show ++ Cord("}")
          case Then(f, s) => f.show ++ Cord(" ≺ ") ++ s.show
        }
      }
    }
}

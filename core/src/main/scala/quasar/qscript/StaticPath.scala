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

package quasar.qscript

import quasar.Planner.PlannerError
import quasar.fp._

import scala.inline

import matryoshka._, Recursive.ops._, TraverseT.ops._
import matryoshka.patterns.CoEnv
import pathy.Path._
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait StaticPath[F[_]] {
  type IT[F[_]]

  implicit def R = scala.Predef.implicitly[Const[Read, ?] :<: QScriptTotal[IT, ?]]

  def pathifyƒ[G[_]: Traverse](f: StaticPathTransformation[IT, G])(
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             PF: Pathable[IT, ?] ~> G,
             FG: F ~> G,
             FI: G :<: QScriptTotal[IT, ?],
             F: Traverse[F]):
      AlgebraM[PlannerError \/ ?, F, IT[QScriptTotal[IT, ?]] \/ IT[Pathable[IT, ?]]]

  def readFile[F[_], G[_]](implicit R: Const[Read, ?] :<: G, F: F ~> G):
      CoEnv[AbsFile[Sandboxed], F, ?] ~> G =
    new (CoEnv[AbsFile[Sandboxed], F, ?] ~> G) {
      def apply[A](coe: CoEnv[AbsFile[Sandboxed], F, A]) =
        coe.run.fold(file => R.inj(Const(Read(file))), F)
    }

  /** Applied after the backend-supplied function, to convert the files into
    * reads and combine them as necessary.
    */
  def postPathify[F[_], G[_]: Functor](
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             R: Const[Read, ?] :<: G,
             FG: F ~> G,
             DE: Const[DeadEnd, ?] :<: G,
             SP: SourcedPathable[IT, ?] :<: G,
             FI: G :<: QScriptTotal[IT, ?]):
      AlgebraicTransform[IT, Pathed[F, ?], G] =
    p => {
      val newP = p.map(readFile[F, G])
      newP.tail.foldRight(
        newP.head)(
        (elem, acc) => SP.inj(Union(DE.inj(Const[DeadEnd, IT[G]](Root)).embed, elem.embed.cata((g: G[Free[QScriptTotal[IT, ?], Hole]]) => Free.roll[QScriptTotal[IT, ?], Hole](FI.inj(g))), acc.embed.cata((g: G[Free[QScriptTotal[IT, ?], Hole]]) => Free.roll[QScriptTotal[IT, ?], Hole](FI.inj(g))))))
    }

  def toRead[F[_]: Traverse, G[_]: Functor](
    f: StaticPathTransformation[IT, F])(
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             F: Pathable[IT, ?] ~> F,
             R: Const[Read, ?] :<: G,
             FG: F ~> G,
             DE: Const[DeadEnd, ?] :<: G,
             SP: SourcedPathable[IT, ?] :<: G,
             FI: G :<: QScriptTotal[IT, ?]):
      IT[Pathable[IT, ?]] => PlannerError \/ IT[G] =
    _.transCataM[PlannerError \/ ?, Pathed[F, ?]](f) ∘ (FunctorT[IT].transCata[Pathed[F, ?], G](_)(postPathify[F, G]))
}

object StaticPath extends LowPriorityStaticPathInstances {
  type Aux[T[_[_]], F[_]] = StaticPath[F] { type IT[F[_]] = T[F] }

  implicit def pathable[T[_[_]], F[_]](implicit Path: F :<: Pathable[T, ?]):
      StaticPath.Aux[T, F] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]

      def pathifyƒ[G[_]: Traverse](f: StaticPathTransformation[T, G])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 F: F ~> G,
                 FI: G :<: QScriptTotal[T, ?], T: Traverse[F]):
          // AlgebraM[PlannerError \/ ?, F, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]]
          F[T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]] => PlannerError \/ (T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]) =
        fa => fa.sequence.bimap(qt => FI(F(fa.as(qt))).embed, Path(_).embed).right
    }

  implicit def coproduct[T[_[_]], H[_]: Traverse, I[_]: Traverse](
    implicit FS: StaticPath.Aux[T, H], GS: StaticPath.Aux[T, I]):
      StaticPath.Aux[T, Coproduct[H, I, ?]] =
    new StaticPath[Coproduct[H, I, ?]] {
      type IT[F[_]] = T[F]

      def pathifyƒ[G[_]: Traverse](f: StaticPathTransformation[T, G])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 F: Coproduct[H, I, ?] ~> G,
                 FI: G :<: QScriptTotal[T, ?],
                 T: Traverse[Coproduct[H, I, ?]]):
          AlgebraM[PlannerError \/ ?, Coproduct[H, I, ?], T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]] =
        _.run.fold(
          FS.pathifyƒ(f)(Traverse[G], TC, TR, PF, F.compose(Inject[H, Coproduct[H, I, ?]]), FI, Traverse[H]),
          GS.pathifyƒ(f)(Traverse[G], TC, TR, PF, F.compose(Inject[I, Coproduct[H, I, ?]]), FI, Traverse[I]))
    }
}

sealed trait LowPriorityStaticPathInstances {
  implicit def default[T[_[_]], F[_]]: StaticPath.Aux[T, F] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]

      def pathifyƒ[G[_]: Traverse](f: StaticPathTransformation[T, G])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 F: F ~> G,
                 FI: G :<: QScriptTotal[T, ?],
                 T: Traverse[F]):
          AlgebraM[PlannerError \/ ?, F, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]]=
        _.traverse(_.fold[PlannerError \/ T[QScriptTotal[T, ?]]](
          _.right,
          toRead[G, QScriptTotal[T, ?]](f)))
          .map(x => FI(F(x)).embed.left)
    }
}

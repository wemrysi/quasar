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

package quasar.qscript.qsu

import slamdata.Predef._

import quasar.NameGenerator
import quasar.common.JoinType
import quasar.ejson.implicits._
import quasar.fp.{coproductEqual, symbolOrder}
import quasar.qscript.{
  construction,
  Center,
  JoinSide,
  LeftSide3,
  LeftSideF,
  RightSide3,
  RightSideF}
import quasar.qscript.provenance.Dimensions
import quasar.qscript.MapFuncsCore.StrLit

import matryoshka._
import matryoshka.data.free._
import scalaz.{Monad, WriterT}
import scalaz.Scalaz._

final class ReifyAutoJoins[T[_[_]]: BirecursiveT: EqualT] extends QSUTTypes[T] {
  import ApplyProvenance.AuthenticatedQSU
  import QSUGraph.Extractors._

  def apply[F[_]: Monad: NameGenerator](qsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    qsu.graph.rewriteM(reifyAutoJoins[F](qsu.dims)).run map {
      case (additionalDims, newGraph) =>
        val newDims = additionalDims.foldLeft(qsu.dims)(_ + _)
        AuthenticatedQSU(newGraph, newDims)
    }
  }

  ////

  private val prov = new QProv[T]
  private val qsu  = QScriptUniform.Optics[T]
  private val func = construction.Func[T]

  private type QSU[A] = QScriptUniform[A]
  private type P = prov.P
  private type DimsT[F[_], A] = WriterT[F, List[(Symbol, Dimensions[P])], A]

  private def reifyAutoJoins[F[_]: Monad: NameGenerator](dims: QSUDims[T])
      : PartialFunction[QSUGraph, DimsT[F, QSUGraph]] = {

    case g @ AutoJoin2(left, right, combiner) =>
      val (l, r) = (left.root, right.root)

      val condition: FreeAccess[JoinSide] =
        prov.autojoinCondition(dims(l), dims(r))

      g.overwriteAtRoot(qsu.thetaJoin(
        l, r, condition, JoinType.Inner, combiner)).point[DimsT[F, ?]]

    case g @ AutoJoin3(left, center, right, combiner3) =>
      val (l, c, r) = (left.root, center.root, right.root)

      WriterT((NameGenerator[F].prefixedName("autojoin") |@|
        NameGenerator[F].prefixedName("leftAccess") |@|
        NameGenerator[F].prefixedName("centerAccess")) {

        case (joinName, lName, cName) =>

          val lcCondition: FreeAccess[JoinSide] =
            prov.autojoinCondition(dims(l), dims(c))

          def lcCombiner: JoinFunc =
            func.ConcatMaps(
              func.MakeMap(StrLit(lName), LeftSideF),
              func.MakeMap(StrLit(cName), RightSideF))

          def lcJoin: QSU[Symbol] =
            qsu.thetaJoin(l, c, lcCondition, JoinType.Inner, lcCombiner)

          def projLeft[A](hole: FreeMapA[A]): FreeMapA[A] =
            func.ProjectKey(StrLit(lName), hole)

          def projCenter[A](hole: FreeMapA[A]): FreeMapA[A] =
            func.ProjectKey(StrLit(cName), hole)

          val combiner: JoinFunc = combiner3 flatMap {
            case LeftSide3 => projLeft[JoinSide](LeftSideF)
            case Center => projCenter[JoinSide](LeftSideF)
            case RightSide3 => RightSideF
          }

          val lcDims: Dimensions[P] =
            prov.join(dims(l), dims(c))

          val condition: FreeAccess[JoinSide] =
            prov.autojoinCondition(lcDims, dims(r))

          val lcName = Symbol(joinName)
          val lcG = QSUGraph.vertices[T].modify(_.updated(lcName, lcJoin))(g)
          val lcrJoin = qsu.thetaJoin(lcName, r, condition, JoinType.Inner, combiner)

          (List(lcName -> lcDims), lcG.overwriteAtRoot(lcrJoin))
      })
  }
}

object ReifyAutoJoins {
  def apply[T[_[_]]: BirecursiveT: EqualT]: ReifyAutoJoins[T] =
    new ReifyAutoJoins[T]
}

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

import slamdata.Predef.{Map => SMap, _}

import quasar.Planner.{PlannerErrorME, InternalError}
import quasar.contrib.scalaz.{MonadState_, MonadTell_}
import quasar.ejson
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{ExcludeId, HoleF, IdOnly, IdStatus, RightSideF}
import quasar.qscript.provenance._

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import pathy.Path
import scalaz.{DList, Free, IList, Monad, Show, StateT, WriterT}
import scalaz.syntax.foldable1._
import scalaz.syntax.monad._
import scalaz.syntax.show._

final class ApplyProvenance[T[_[_]]: BirecursiveT: EqualT] private () {
  import ApplyProvenance._
  import QScriptUniform._

  type P = QProv.P[T]
  type Dims = Dimensions[P]
  type QSU[A] = QScriptUniform[T, A]

  type GPF[A] = QSUGraph.QSUPattern[T, A]
  val GPF = QSUGraph.QSUPattern

  type GStateM[F[_]] = MonadState_[F, QSUGraph[T]]
  def GStateM[F[_]](implicit ev: GStateM[F]): GStateM[F] = ev

  type RenameT[F[_]] = MonadTell_[F, DList[(Symbol, Symbol)]]
  def RenameT[F[_]](implicit ev: RenameT[F]): RenameT[F] = ev

  val dims = QProv[T]

  def apply[F[_]: Monad: PlannerErrorME](graph: QSUGraph[T]): F[AuthenticatedQSU[T]] = {
    type X[A] = WriterT[StateT[F, QSUGraph[T], ?], DList[(Symbol, Symbol)], A]

    def applyRenames(renames: IList[(Symbol, Symbol)], ds: Dimensions[P]): Dimensions[P] =
      renames.foldLeft(ds)((ds0, t) => dims.rename(t._1, t._2, ds0))

    graph.elgotZygoM(computeProvenanceƒ[X], applyProvenanceƒ[X])
      .run
      .run(graph)
      .map { case (g, (renames, (_, ds))) =>
        AuthenticatedQSU(g, ds mapValues (applyRenames(renames.toIList, _)))
      }
  }

  def applyProvenanceƒ[F[_]: Monad: GStateM: RenameT]: ElgotAlgebraM[((Symbol, Dims), ?), F, GPF, (Symbol, QSUDims[T])] = {
    case ((_, updDims), GPF(deId, DimEdit((srcId, qdims), _))) =>
      for {
        _ <- GStateM[F].modify(_.replace(deId, srcId))
        _ <- RenameT[F].tell(DList(deId -> srcId))
      } yield (srcId, qdims + (srcId -> updDims))

    case ((_, tdims), GPF(tid, Transpose((srcId, srcDims), retain, rot))) =>
      GStateM[F].modify(
        QSUGraph.vertices.modify(_.updated(
          tid,
          LeftShift(srcId, HoleF, retain.fold[IdStatus](IdOnly, ExcludeId), RightSideF, rot))))
        .as((tid, srcDims + (tid -> tdims)))

    case ((_, nodeDims), GPF(nodeId, node)) =>
      (nodeId, node.foldRight[QSUDims[T]](SMap(nodeId -> nodeDims))(_._2 ++ _)).point[F]
  }

  def computeProvenanceƒ[F[_]: Monad: PlannerErrorME]: AlgebraM[F, GPF, (Symbol, Dims)] = {
    case GPF(root, qsu) =>
      val computedDims: F[Dims] = qsu match {
        case AutoJoin2((_, left), (_, right), _) => dims.join(left, right).point[F]

        case AutoJoin3((_, left), (_, center), (_, right), _) => dims.join(dims.join(left, center), right).point[F]

        case DimEdit((_, src), DTrans.Squash()) => dims.squash(src).point[F]

        case DimEdit((name, src), DTrans.Group(k)) =>
          dims.swap(0, 1, dims.lshift(k as Access.value(name), src)).point[F]

        case Distinct((_, src)) => src.point[F]

        case GroupBy(_, _) => unexpectedError("GroupBy", root)

        case JoinSideRef(_) => unexpectedError("JoinSideRef", root)

        case LeftShift(_, _, _, _, _) => unexpectedError("LeftShift", root)

        case LPFilter(_, _) => unexpectedError("LPFilter", root)

        case LPJoin(_, _, _, _, _, _) => unexpectedError("LPJoin", root)

        case LPReduce((_, src), _) => dims.bucketAccess(root, dims.reduce(src)).point[F]

        case LPSort(_, _) => unexpectedError("LPSort", root)

        case QSFilter((_, src), _) => src.point[F]

        case QSReduce((_, src), _, _, _) => dims.bucketAccess(root, dims.reduce(src)).point[F]

        case QSSort((_, src), _, _) => src.point[F]

        case Unary(_, _) => unexpectedError("Unary", root)

        case Map((_, src), _) => src.point[F]

        case Read(file) => dims.squash(segments(file).map(projStr).reverse).point[F]

        case Subset((_, from), _, (_, count)) => dims.join(from, count).point[F]

        case ThetaJoin((_, left), (_, right), _, _, _) => dims.join(left, right).point[F]

        case Transpose((_, src), _, rot) =>
          val tid: dims.I = Free.pure(Access.identity(root, root))
          (rot match {
            case Rotation.ShiftMap   | Rotation.ShiftArray   => dims.lshift(tid, src)
            case Rotation.FlattenMap | Rotation.FlattenArray => dims.flatten(tid, src)
          }).point[F]

        case Union((_, left), (_, right)) => dims.union(left, right).point[F]

        case Unreferenced() => dims.empty.point[F]
      }

      computedDims strengthL root
  }

  ////

  private def projStr(s: String): P =
    dims.prov.proj(ejson.CommonEJson(ejson.Str[T[EJson]](s)).embed)

  private def segments(p: Path[_, _, _]): IList[String] = {
    val segs = Path.flatten[IList[String]](IList(), IList(), IList(), IList(_), IList(_), p)
    (segs.head :: segs.tail).join
  }

  private def unexpectedError[F[_]: PlannerErrorME, A](nodeName: String, id: Symbol): F[A] =
    PlannerErrorME[F].raiseError(InternalError(s"Encountered unexpected $nodeName[$id].", None))
}

object ApplyProvenance {
  def apply[
      T[_[_]]: BirecursiveT: EqualT,
      F[_]: Monad: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ApplyProvenance", new ApplyProvenance[T].apply[F](graph))

  def computeProvenanceƒ[
      T[_[_]]: BirecursiveT: EqualT,
      F[_]: Monad: PlannerErrorME]
      : AlgebraM[F, QSUGraph.QSUPattern[T, ?], (Symbol, Dimensions[QProv.P[T]])] =
    new ApplyProvenance[T].computeProvenanceƒ[F]

  final case class AuthenticatedQSU[T[_[_]]](
      graph: QSUGraph[T],
      dims: QSUDims[T])

  object AuthenticatedQSU {
    implicit def show[T[_[_]]: ShowT]: Show[AuthenticatedQSU[T]] =
      Show.shows { case AuthenticatedQSU(g, d) =>
        s"AuthenticatedQSU {\n" +
        g.shows +
        "\n\n" +
        QSUDims.show[T].shows(d) +
        "\n}"
      }
  }
}

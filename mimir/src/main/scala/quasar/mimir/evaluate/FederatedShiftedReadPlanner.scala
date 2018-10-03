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

package quasar.mimir.evaluate

import slamdata.Predef.{Stream => _, _}

import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.impl.evaluate.{Source => EvalSource}
import quasar.mimir._, MimirCake._
import quasar.precog.common.RValue
import quasar.qscript._, PlannerError.InternalError
import quasar.yggdrasil.{MonadFinalizers, TransSpecModule}

import cats.effect.{IO, LiftIO}
import fs2.{Segment, Stream}
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

import scala.concurrent.ExecutionContext

final class FederatedShiftedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: LiftIO: Monad: MonadPlannerErr: MonadFinalizers[?[_], IO]](
    val P: Cake) {

  import Shifting.ShiftInfo

  type Assocs = Associates[T, IO]
  type M[A] = AssociatesT[T, F, IO, A]

  type Read[A] = Const[ShiftedRead[AFile], A] \/ Const[ExtraShiftedRead[AFile], A]

  def plan(implicit ec: ExecutionContext)
      : AlgebraM[M, Read, MimirRepr] = {
    case -\/(Const(ShiftedRead(file, status))) =>
      planRead(file, status, None)

    case \/-(Const(ExtraShiftedRead(file, shiftPath, shiftStatus, shiftType, shiftKey))) =>
      planRead(file, ExcludeId, Some(ShiftInfo(shiftPath, shiftStatus, shiftType, shiftKey)))
  }

  ////

  private val dsl = construction.mkGeneric[T, QScriptRead[T, ?]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private def planRead(file: AFile, readStatus: IdStatus, shiftInfo: Option[ShiftInfo])(
      implicit ec: ExecutionContext)
      : M[MimirRepr] =
    Kleisli.ask[F, Assocs].map(_(file)) andThenK { maybeSource =>
      for {
        source <- MonadPlannerErr[F].unattempt_(
          maybeSource \/> InternalError.fromMsg(s"No source for '${posixCodec.printPath(file)}'."))

        tbl <- sourceTable(source, shiftInfo)

        repr = MimirRepr(P)(tbl)
      } yield {
        import repr.P.trans._

        readStatus match {
          case IdOnly =>
            repr.map(_.transform(constants.SourceKey.Single))

          case IncludeId =>
            val ids = constants.SourceKey.Single
            val values = constants.SourceValue.Single

            // note that ids are already an array
            repr.map(_.transform(InnerArrayConcat(ids, WrapArray(values))))

          case ExcludeId =>
            repr.map(_.transform(constants.SourceValue.Single))
        }
      }
    }

  private def sourceTable(
      source: EvalSource[QueryAssociate[T, IO]],
      shiftInfo: Option[ShiftInfo])(
      implicit ec: ExecutionContext)
      : F[P.Table] = {
    val queryResult =
      source.src match {
        case QueryAssociate.Lightweight(f) =>
          f(source.path)

        case QueryAssociate.Heavyweight(f) =>
          val shiftedRead =
            dsl.LeftShift(
              refineType(source.path.toPath)
                .fold(dsl.Read(_), dsl.Read(_)),
              recFunc.Hole,
              ExcludeId,
              ShiftType.Map,
              OnUndefined.Omit,
              func.RightSide)

          f(shiftedRead)
      }

    queryResult.to[F].flatMap(tableFromStream(_, shiftInfo))
  }

  // we do not preserve the order of shifted results
  private def tableFromStream(
      rvalues: Stream[IO, RValue],
      shift: Option[ShiftInfo])(
      implicit ec: ExecutionContext)
      : F[P.Table] = {

    val shiftedRValues: Stream[IO, RValue] =
      shift match {
        case None => rvalues
        case Some(shiftInfo) =>
          rvalues.mapChunks(chunk =>
            Segment.seq(chunk.foldLeft(List[RValue]()) {
              case (acc, rv) => Shifting.shiftRValue(rv, shiftInfo) ::: acc
            }))
      }

    P.Table.fromQDataStream[F, RValue](shiftedRValues) map { table =>
      import P.trans._

      // TODO depending on the id status we may not need to wrap the table
      table.transform(OuterObjectConcat(
        WrapObject(
          Scan(Leaf(Source), P.freshIdScanner),
          TransSpecModule.paths.Key.name),
        WrapObject(
          Leaf(Source),
          TransSpecModule.paths.Value.name)))
    }
  }
}

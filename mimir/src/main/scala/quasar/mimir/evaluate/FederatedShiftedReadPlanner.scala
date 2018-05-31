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

import quasar.{Data, Disposable}
import quasar.api._, ResourceError._
import quasar.blueeyes.json.JValue
import quasar.contrib.fs2.convert
import quasar.contrib.pathy._
import quasar.evaluate.{Source => EvalSource}
import quasar.fs.PathError
import quasar.fs.Planner.{InternalError, PlannerErrorME, PlanPathError}
import quasar.mimir._, MimirCake._
import quasar.precog.common.RValue
import quasar.qscript._
import quasar.yggdrasil.TransSpecModule.paths.{Key, Value}
import quasar.yggdrasil.UnknownSize
import quasar.yggdrasil.table.Slice

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import delorean._
import fs2.{Chunk, Stream}
import fs2.interop.scalaz._
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class FederatedShiftedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: Monad: PlannerErrorME](
    val P: Cake,
    liftTask: Task ~> F) {

  type Assocs = Associates[T, F, Task]
  type M[A] = AssociatesT[T, F, Task, A]

  val plan: AlgebraM[M, Const[ShiftedRead[AFile], ?], MimirRepr] = {
    case Const(ShiftedRead(file, status)) =>
      Kleisli.ask[F, Assocs].map(_(file)) andThenK { maybeSource =>
        for {
          source <- PlannerErrorME[F].unattempt_(
            maybeSource \/> InternalError.fromMsg(s"No source for '${posixCodec.printPath(file)}'."))

          tbl <- sourceTable(source)

          repr = MimirRepr(P)(tbl)
        } yield {
          import repr.P.trans._

          status match {
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
  }

  ////

  private val dsl = construction.mkGeneric[T, QScriptRead[T, ?]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private def sourceTable(source: EvalSource[QueryAssociate[T, F, Task]]): F[P.Table] = {
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

    queryResult.flatMap(_.fold(handleReadError[P.Table], tableFromStream))
  }

  private def tableFromStream(d: Disposable[Task, Stream[Task, Data]]): F[P.Table] =
    liftTask(Task.delay {
      val sliceStream =
        d.value
          .onFinalize(d.dispose)
          .map(data => RValue.fromJValueRaw(JValue.fromData(data)))
          .chunks
          .map(c => Slice.fromRValues(unfold(c: Chunk[RValue])(_.uncons)))

      val slices =
        convert.toStreamT(sliceStream).trans(λ[Task ~> Future](_.unsafeToFuture))

      import P.trans._

      // TODO depending on the id status we may not need to wrap the table
      P.Table(slices, UnknownSize)
        .transform(OuterObjectConcat(
          WrapObject(
            Scan(Leaf(Source), P.freshIdScanner),
            Key.name),
          WrapObject(
            Leaf(Source),
            Value.name)))
    })

  private def handleReadError[A](rerr: ReadError): F[A] =
    PlannerErrorME[F].raiseError[A](PlanPathError(rerr match {
      case NotAResource(rp) =>
        PathError.invalidPath(rp.toPath, "does not refer to a resource.")

      case PathNotFound(rp) =>
        PathError.pathNotFound(rp.toPath)
    }))
}

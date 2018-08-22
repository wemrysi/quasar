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

package quasar.qsu

import quasar.qscript.{
  construction,
  MFC,
  MFD,
  MapFuncsCore,
  MapFuncsDerived
}
import quasar.RenderTreeT
import quasar.Type
import quasar.contrib.iota._
import quasar.qscript.RecFreeS._
import quasar.qscript.{Hole, MonadPlannerErr}
import quasar.qsu.{QScriptUniform => QSU}
import ApplyProvenance.AuthenticatedQSU

import matryoshka.{Recursive, Corecursive}
import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.syntax.applicative._
import scalaz.syntax.bind._
import scalaz.syntax.std.option._
import scalaz.{\/-, Applicative}

final class ElideStructGuards[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  private def func = construction.Func[T]

  def apply(qgraph0: QSUGraph): QSUGraph =
    qgraph0 rewrite {
      case qgraph @ LeftShift(MappableRegion.MaximalUnary(source, struct2), struct, idStatus, onUndefined, repair, rotation) =>
        val structAdj0 = struct.linearize >> struct2
        val structAdj = elideGuards(structAdj0)

        qgraph.overwriteAtRoot(QSU.LeftShift(source.root, structAdj.asRec, idStatus, onUndefined, repair, rotation))
    }

  private def elideGuards(fm: FreeMap)(
    implicit R: Recursive.Aux[FreeMap, CoEnv[Hole, MapFunc, ?]],
             C: Corecursive.Aux[FreeMap, CoEnv[Hole, MapFunc, ?]]): FreeMap =
    R.cata[FreeMap](fm) {
      case CoEnv(\/-(MFD(MapFuncsDerived.Typecheck(result, tpe)))) =>
        func.Typecheck(result, simplifyType(tpe))
      case CoEnv(\/-(MFC(MapFuncsCore.Guard(check, tpe, result, otherwise)))) =>
        func.Guard(check, simplifyType(tpe), result, otherwise)
      case otherwise => C.embed(otherwise)
    }

  private def simplifyType(tpe: Type): Type =
    tpe match {
      case Type.FlexArr(minSize, maxSize, _) => Type.FlexArr(minSize, maxSize, Type.Top)
      case Type.Obj(value, _) => Type.Obj(value, Type.Top.some)
      case otherwise => otherwise
    }
}

object ElideStructGuards {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, F[_]: Applicative: MonadPlannerErr]
      (aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError(
      "ElideStructGuards",
      AuthenticatedQSU[T](
        new ElideStructGuards[T].apply(aqsu.graph), aqsu.auth).point[F])
}

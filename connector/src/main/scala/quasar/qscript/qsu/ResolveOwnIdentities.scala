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

import matryoshka.{Hole => _, _}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.{construction, IdOnly, IdStatus, IncludeId, ExcludeId, SrcHole}
import QSUGraph.Extractors._
import quasar.fp.ski._
import scalaz._, Scalaz._

final class ResolveOwnIdentities[T[_[_]]: BirecursiveT: ShowT] private () extends QSUTTypes[T] {

  val func = construction.Func[T]

  def apply(qgraph: QSUGraph): QSUGraph = {
    qgraph.rewrite {
      case qg @ LeftShift(source, struct, idStatus, repair, rotation)
        if repair.element(QSU.AccessLeftTarget(Access.identityHole((source.root, SrcHole)))) =>
        val newRepair = repair.flatMap {
          case QSU.AccessLeftTarget(Access.identityHole((symbol, _))) if symbol == qg.root =>
            func.ProjectIndexI(func.RightTarget, 0)
          case QSU.AccessLeftTarget(access) =>
            func.AccessLeftTarget(κ(access))
          case QSU.RightTarget =>
            if (idStatus === ExcludeId)
              func.ProjectIndexI(func.RightTarget, 1)
            else
              func.RightTarget
        }
        val newIdStatus: IdStatus =
          if (idStatus === IdOnly) IdOnly else IncludeId
        qg.overwriteAtRoot(QSU.LeftShift(source.root, struct, newIdStatus, newRepair, rotation))
    }
  }
}

object ResolveOwnIdentities {
  def apply[T[_[_]]: BirecursiveT: ShowT](qgraph: QSUGraph[T]): QSUGraph[T] = new ResolveOwnIdentities[T].apply(qgraph)
}

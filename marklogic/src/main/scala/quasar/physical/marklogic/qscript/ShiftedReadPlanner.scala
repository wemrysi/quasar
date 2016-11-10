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

package quasar.physical.marklogic.qscript

import quasar.NameGenerator
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import pathy.Path._
import scalaz._

private[qscript] final class ShiftedReadPlanner[F[_]: NameGenerator: PrologW]
  extends MarkLogicPlanner[F, Const[ShiftedRead, ?]] {

  val plan: AlgebraM[F, Const[ShiftedRead, ?], XQuery] = {
    case Const(ShiftedRead(absFile, idStatus)) =>
      val asDir = fileParent(absFile) </> dir(fileName(absFile).value)
      val dirRepr = posixCodec.printPath(asDir)

      val includeId = idStatus match {
        case IncludeId => fn.True
        case ExcludeId => fn.False
        // FIXME: This ain’t right.
        case IdOnly    => fn.False
      }

      qscript.shiftedRead apply (dirRepr.xs, includeId)
  }
}

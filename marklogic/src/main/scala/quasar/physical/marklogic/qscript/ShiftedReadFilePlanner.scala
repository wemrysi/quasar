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

import quasar.contrib.pathy.AFile
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

private[qscript] final class ShiftedReadFilePlanner[F[_]: QNameGenerator: PrologW]
  extends MarkLogicPlanner[F, Const[ShiftedRead[AFile], ?]] {

  import expr._, axes.child

  val plan: AlgebraM[F, Const[ShiftedRead[AFile], ?], XQuery] = {
    case Const(ShiftedRead(absFile, idStatus)) =>
      for {
        d     <- freshName[F]
        c     <- freshName[F]
        b     <- freshName[F]
        uri   =  posixCodec.printPath(fileParent(absFile) </> dir(fileName(absFile).value))
        xform <- json.transformFromJson[F](~c)
        incId <- ejson.seqToArray_[F](mkSeq_(
                   fn.concat("_".xs, xdmp.hmacSha1("quasar".xs, fn.documentUri(~d))),
                   ~b))
      } yield {
        for_(
          d in cts.search(fn.doc(), cts.directoryQuery(uri.xs, "1".xs)))
        .let_(
          c := ~d `/` child.node(),
          b := (if_ (json.isObject(~c)) then_ xform else_ ~c))
        .return_(idStatus match {
          case IncludeId => incId
          case ExcludeId => ~b
        })
      }
  }
}

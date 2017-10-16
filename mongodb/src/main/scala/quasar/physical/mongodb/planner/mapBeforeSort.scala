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

package quasar.physical.mongodb.planner

import slamdata.Predef.{Map => _, _}
import quasar.fp._
import quasar.qscript.{MapFuncsCore => MF, _}

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import scalaz._, Scalaz._

object mapBeforeSort {

  def apply[T[_[_]]: BirecursiveT]: Trans[T] =
    new Trans[T] {
      def trans[F[_], G[_]: Functor]
          (GtoF: PrismNT[G, F])
          (implicit QC: QScriptCore[T, ?] :<: F): QScriptCore[T, T[G]] => F[T[G]] = {
        case qs @ Map(Embed(src), fm) =>
          GtoF.get(src) >>= QC.prj match {
            case Some(Sort(innerSrc, bucket, order)) =>
              val innerMap =
                GtoF.reverseGet(QC.inj(Map(
                  innerSrc,
                  MapFuncCore.StaticArray(List(fm, HoleF[T]))))).embed
              QC.inj(Map(
                GtoF.reverseGet(QC.inj(Sort(innerMap,
                  bucket.map(_ >> Free.roll[MapFunc[T, ?], Hole](MFC(MF.ProjectIndex(HoleF[T], MF.IntLit(1))))),
                  order.map {
                    case (fm, dir) =>
                      (fm >> Free.roll[MapFunc[T, ?], Hole](MFC(MF.ProjectIndex(HoleF[T], MF.IntLit(1)))), dir)
                  }))).embed,
                Free.roll(MFC(MF.ProjectIndex(HoleF[T], MF.IntLit(0))))))
            case _ => QC.inj(qs)
          }
        case x => QC.inj(x)
      }
  }
}

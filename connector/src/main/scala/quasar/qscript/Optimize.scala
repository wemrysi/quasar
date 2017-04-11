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

package quasar.qscript

import slamdata.Predef._
import quasar.fp._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

class Optimize[T[_[_]]: BirecursiveT] extends TTypes[T] {
  /** Pull more work to _after_ count operations, limiting the dataset. */
  // TODO: we should be able to pull _most_ of a Reduce repair function to after a Subset
  private def swapMapSubset[F[_], G[_]: Functor]
    (FtoG: F ~> G)
    (implicit QC: QScriptCore :<: F)
      : QScriptCore[T[G]] => Option[QScriptCore[T[G]]] = {

    val FI = Injectable.inject[QScriptCore, QScriptTotal]

    {
      case Subset(src, from, sel, count) =>
        from.resume.swap.toOption >>= (FI project _) >>= {
          case Map(fromInner, mf) =>
            Map(FtoG(QC.inj(Subset(src, fromInner, sel, count))).embed, mf).some
          case _ => None
        }
      case _ => None
    }
  }

  /** Should only be applied after all other QScript transformations. This gives
    * the final, optimized QScript for conversion.
    */
  def optimize[F[_], G[_]: Functor](FtoG: F ~> G)(implicit QC: QScriptCore :<: F)
      : F[T[G]] => F[T[G]] =
    liftFF(repeatedly(swapMapSubset(FtoG)))
}

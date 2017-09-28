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

import slamdata.Predef._
import quasar.physical.mongodb.expression._
import quasar.qscript._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

trait StaticHandler {
  def handle[T[_[_]]: BirecursiveT, M[_]: Monad, A]
    (alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]])
    (fm: FreeMapA[T, A])
      : Option[M[Fix[ExprOp]]]
}

object StaticHandler {

  def handleOpsCore[T[_[_]]: BirecursiveT, M[_]: Monad, EX[_]: Traverse, A]
    (alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]])
    (fm: FreeMapA[T, A])
    (implicit e26: ExprOpCoreF :<: EX)
      : Option[M[Fix[ExprOp]]] = None

  def handleOps3_2[T[_[_]]: BirecursiveT, M[_]: Monad, EX[_]: Traverse, A]
    (alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]])
    (fm: FreeMapA[T, A])
    (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
      : Option[M[Fix[ExprOp]]] = {
    val fp32 = new ExprOp3_2F.fixpoint[Fix[ExprOp], ExprOp](Fix(_))
    fm.project.some collect {
      case MapFuncCore.StaticArray(a) =>
        val cs: M[List[Fix[ExprOp]]] = a.map(_.cataM(alg)).sequence
        cs.map(fp32.$arrayLit)
    }
  }

  object v2_6 {
    def apply(): StaticHandler =
      new StaticHandler {

        def handle[T[_[_]]: BirecursiveT, M[_]: Monad, A]
          (alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]])
          (fm: FreeMapA[T, A])
            : Option[M[Fix[ExprOp]]] =
          handleOpsCore[T, M, Expr2_6, A](alg)(fm)
      }
  }

  object v3_2 {
    def apply(): StaticHandler =
      new StaticHandler {

        def handle[T[_[_]]: BirecursiveT, M[_]: Monad, A]
          (alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[ExprOp]])
          (fm: FreeMapA[T, A])
            : Option[M[Fix[ExprOp]]] =
          handleOps3_2[T, M, Expr3_2, A](alg)(fm)
      }
  }
}

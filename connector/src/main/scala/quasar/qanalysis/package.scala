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

package quasar

import slamdata.Predef._
import quasar.qscript._
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.fp._, ski._

import matryoshka.{Hole => _, _}
import matryoshka.patterns._
import matryoshka.implicits._
import matryoshka.data._
import scalaz._, Scalaz._
import simulacrum.typeclass

package object qanalysis {

  sealed trait AnalysisResult
  case object Instant extends AnalysisResult
  case object Interactive extends AnalysisResult
  case object SemiInteractive extends AnalysisResult
  case object Batch extends AnalysisResult
  case object Unknown extends AnalysisResult

  @typeclass
  trait Cardinality[F[_]] {
    def calculate(pathCard: APath => Int): Algebra[F, Int]
  }

  @typeclass
  trait Cost[F[_]] {
    def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), F, Int]
  }

  @typeclass
  trait Analyzer[F[_]] {
    def analyze[T](t: T)(pathCard: APath => Int)(implicit
      rec: Recursive.Aux[T, F],
      func: Functor[F],
      cardinalty: Cardinality[F],
      cost: Cost[F]
    ): Int = rec.zygo(t)(cardinalty.calculate(pathCard), cost.evaluate(pathCard))
  }

  object Cardinality {

    implicit def read[A]: Cardinality[Const[Read[A], ?]] =
      new Cardinality[Const[Read[A], ?]] {
        def calculate(pathCard: APath => Int): Algebra[ Const[Read[A], ?], Int] =
          (qs: Const[Read[A], Int]) => 1
      }
    implicit def shiftedReadFile: Cardinality[Const[ShiftedRead[AFile], ?]] =
      new Cardinality[Const[ShiftedRead[AFile], ?]] {
        def calculate(pathCard: APath => Int): Algebra[ Const[ShiftedRead[AFile], ?], Int] =
          (qs: Const[ShiftedRead[AFile], Int]) => pathCard(qs.getConst.path)
      }
    implicit def shiftedReadDir: Cardinality[Const[ShiftedRead[ADir], ?]] =
      new Cardinality[Const[ShiftedRead[ADir], ?]] {
        def calculate(pathCard: APath => Int): Algebra[ Const[ShiftedRead[ADir], ?], Int] =
          (qs: Const[ShiftedRead[ADir], Int]) => pathCard(qs.getConst.path)
      }
    implicit def qscriptCore[T[_[_]]: RecursiveT: ShowT]: Cardinality[QScriptCore[T, ?]] =
      new Cardinality[QScriptCore[T, ?]] {

        val I = Inject[QScriptCore[T, ?], QScriptTotal[T, ?]]

        def calculate(pathCard: APath => Int): Algebra[QScriptCore[T, ?], Int] = {
          case qscript.Map(card, f) => card
          case Reduce(card, bucket, reducers, repair) =>
            bucket.fold(κ(card / 2), {
              case MapFuncs.Constant(v) => 1
              case _ => card / 2
            })
          case Sort(card, bucket, orders) => card
          case Filter(card, f) => card / 2
          case Subset(card, from, sel, count) => {
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            def c = count.fold(κ(card / 2), {
              case I(qscript.Map(_, MapFuncs.IntLit(v))) => v.toInt
              case _ => card / 2
            })
            sel match {
              case Drop => card - c
              case _ => c
            }
          }
          case LeftShift(card, struct, id, repair) => card * 10
          case Union(card, lBranch, rBranch) => {
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            lBranch.cata(interpret(κ(card), compile)) + rBranch.cata(interpret(κ(card), compile))
          }
          case Unreferenced() => 1
        }
      }
    implicit def projectBucket[T[_[_]] : RecursiveT: ShowT]: Cardinality[ProjectBucket[T, ?]] =
      new Cardinality[ProjectBucket[T, ?]] {
        def calculate(pathCard: APath => Int): Algebra[ProjectBucket[T, ?], Int] = {
          case BucketField(card, _, _) => card
          case BucketIndex(card, _, _) => card
        }
      }

    implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]: Cardinality[EquiJoin[T, ?]] =
      new Cardinality[EquiJoin[T, ?]] {
        def calculate(pathCard: APath => Int): Algebra[ EquiJoin[T, ?], Int] = {
          case EquiJoin(card, lBranch, rBranch, _, _, _, _) =>
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            lBranch.cata(interpret(κ(card), compile)) * rBranch.cata(interpret(κ(card), compile))
        }
      }

    implicit def thetaJoin[T[_[_]] : RecursiveT : ShowT]: Cardinality[ThetaJoin[T, ?]] =
      new Cardinality[ThetaJoin[T, ?]] {
        def calculate(pathCard: APath => Int): Algebra[ThetaJoin[T, ?], Int] = {
          case ThetaJoin(card, lBranch, rBranch, _, _, _) =>
            val compile = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            lBranch.cata(interpret(κ(card), compile)) * rBranch.cata(interpret(κ(card), compile))
        }
      }

    implicit def deadEnd: Cardinality[Const[DeadEnd, ?]] =
      new Cardinality[Const[DeadEnd, ?]] {
        def calculate(pathCard: APath => Int): Algebra[Const[DeadEnd, ?], Int] = κ(1)
      }

    implicit def coproduct[F[_], G[_]](
      implicit F: Cardinality[F], G: Cardinality[G]):
        Cardinality[Coproduct[F, G, ?]] =
      new Cardinality[Coproduct[F, G, ?]] {
        def calculate(pathCard: APath => Int): Algebra[Coproduct[F, G, ?], Int] =
          _.run.fold(F.calculate(pathCard), G.calculate(pathCard))
      }
  }

  /**
    * This is "generic" implementation for `Cost` that can be used by any connector. 
    * Can be used for newly created connectors. More mature connectors should provide
    * their own instance that will take into account connector specific informations
    */
  object Cost {
    implicit def deadEnd: Cost[Const[DeadEnd, ?]] = new Cost[Const[DeadEnd, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), Const[DeadEnd, ?], Int] =
          (qs: Const[DeadEnd, (Int, Int)]) => 1
      }

    implicit def read[A]: Cost[Const[Read[A], ?]] =
      new Cost[Const[Read[A], ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), Const[Read[A], ?], Int] =
          (qs: Const[Read[A], (Int, Int)]) => 1
      }
    implicit def shiftedRead[A]: Cost[Const[ShiftedRead[A], ?]] =
      new Cost[Const[ShiftedRead[A], ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), Const[ShiftedRead[A], ?], Int] =
          (qs: Const[ShiftedRead[A], (Int, Int)]) => 1
      }
    implicit def qscriptCore[T[_[_]]: RecursiveT: ShowT]: Cost[QScriptCore[T, ?]] =
      new Cost[QScriptCore[T, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), QScriptCore[T, ?], Int] = {
          case qscript.Map((card, cost), f) => card + cost
          case Reduce((card, cost), bucket, reducers, repair) => card + cost
          case Sort((card, cost), bucket, orders) => card + cost
          case Filter((card, cost), f) => card + cost
          case Subset((card, cost), from, sel, count) => card + cost
          case LeftShift((card, cost), struct, id, repair) => card + cost
          case Union((card, cost), lBranch, rBranch) => card + cost
          case Unreferenced() => 0
        }
      }

    implicit def projectBucket[T[_[_]]]: Cost[ProjectBucket[T, ?]] =
      new Cost[ProjectBucket[T, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), ProjectBucket[T, ?], Int] = {
          case BucketField((card, cost), _, _) => card + cost
          case BucketIndex((card, cost), _, _) => card + cost
        }
      }

    // TODO contribute to matryoshka
    private def ginterpret[W[_], F[_], A, B](f: A => Id[B], φ: GAlgebra[W, F, B]): GAlgebra[W, CoEnv[A, F, ?], B] =
      ginterpretM[W, Id, F, A, B](f, φ)

    implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]: Cost[EquiJoin[T, ?]] =
      new Cost[EquiJoin[T, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), EquiJoin[T, ?], Int] = {
          case EquiJoin((card, cost), lBranch, rBranch, lKey, rKey, jt, combine) =>
            val compileCardinality = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            val compileCost = Cost[QScriptTotal[T, ?]].evaluate(pathCard)
            lBranch.zygo(interpret(κ(card), compileCardinality), ginterpret(κ(cost), compileCost)) +
            rBranch.zygo(interpret(κ(card), compileCardinality), ginterpret(κ(cost), compileCost))
        }
      }
    implicit def thetaJoin[T[_[_]] : RecursiveT : ShowT]: Cost[ThetaJoin[T, ?]] =
      new Cost[ThetaJoin[T, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), ThetaJoin[T, ?], Int] = {
          case ThetaJoin((card, cost), lBranch, rBranch, _, _, _) => {
            val compileCardinality = Cardinality[QScriptTotal[T, ?]].calculate(pathCard)
            val compileCost = Cost[QScriptTotal[T, ?]].evaluate(pathCard)
            lBranch.zygo(interpret(κ(card), compileCardinality), ginterpret(κ(cost), compileCost)) +
            rBranch.zygo(interpret(κ(card), compileCardinality), ginterpret(κ(cost), compileCost))
          }
        }
      }

    implicit def coproduct[F[_], G[_]](implicit F: Cost[F], G: Cost[G]):
        Cost[Coproduct[F, G, ?]] =
      new Cost[Coproduct[F, G, ?]] {
        def evaluate(pathCard: APath => Int): GAlgebra[(Int, ?), Coproduct[F, G, ?], Int] =
          _.run.fold(F.evaluate(pathCard), G.evaluate(pathCard))
      }
  }

}

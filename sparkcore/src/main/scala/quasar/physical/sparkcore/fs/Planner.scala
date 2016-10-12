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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar._, quasar.Planner._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.fp.ski._
import quasar.qscript._
import quasar.contrib.pathy.AFile
import quasar.qscript.ReduceFuncs._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import simulacrum.typeclass

@typeclass trait Planner[F[_]] {
  type IT[G[_]]

  def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[Planner.SparkState, F, RDD[Data]]
}

object Planner {

  type SparkState[A] = StateT[EitherT[Task, PlannerError, ?], SparkContext, A]
  type SparkStateT[F[_], A] = StateT[F, SparkContext, A]

  def unimplemented(what: String): SparkState[RDD[Data]] =
    EitherT[Task, PlannerError, RDD[Data]](InternalError(s"unimplemented $what").left[RDD[Data]].point[Task]).liftM[StateT[?[_], SparkContext, ?]]

  type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

  private def unreachable[T[_[_]], F[_]](what: String): Planner.Aux[T, F] =
    new Planner[F] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, F, RDD[Data]] =
        _ =>  StateT((sc: SparkContext) => {
        EitherT(InternalError(s"unreachable $what").left[(SparkContext, RDD[Data])].point[Task])
      })
    }

  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] = unreachable("deadEnd")
  implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] = unreachable("read")
  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] = unreachable("projectBucket")
  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] = unreachable("thetajoin")

  implicit def shiftedread[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead, ?]] =
    new Planner[Const[ShiftedRead, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]) =
        (qs: Const[ShiftedRead, RDD[Data]]) => {
          StateT((sc: SparkContext) => {
            val filePath = qs.getConst.path
            EitherT(fromFile(sc, filePath).map { initRDD =>
              val rdd = initRDD.map { raw =>
                DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
              }
                (sc, rdd).right[PlannerError]

            })
          })
        }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      type IT[G[_]] = T[G]

      type Index = Long
      type Count = Long

      private def filterOut(
        fromFile: (SparkContext, AFile) => Task[RDD[String]],
        src: RDD[Data],
        from: FreeQS[T],
        count: FreeQS[T],
        predicate: (Index, Count) => Boolean ):
          StateT[EitherT[Task, PlannerError, ?], SparkContext, RDD[Data]] = {

        val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
        val srcState = src.point[SparkState]

        val fromState: SparkState[RDD[Data]] = freeCataM(from)(interpretM(κ(srcState), algebraM))
        val countState: SparkState[RDD[Data]] = freeCataM(count)(interpretM(κ(srcState), algebraM))

        val countEval: SparkState[Long] = countState >>= (rdd => EitherT(Task.delay(rdd.first match {
          case Data.Int(v) if v.isValidLong => v.toLong.right[PlannerError]
          case Data.Int(v) => InternalError(s"Provided Integer $v is not a Long").left[Long]
          case a => InternalError(s"$a is not a Long number").left[Long]
        })).liftM[StateT[?[_], SparkContext, ?]])
        (fromState |@| countEval)((rdd, count) =>
          rdd.zipWithIndex.filter(di => predicate(di._2, count)).map(_._1))
      }

      private def reduceData: ReduceFunc[_] => (Data, Data) => Data = {
        case Count(_) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
          case _ => Data.NA
        }
        case Sum(_) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
          case (Data.Number(a), Data.Number(b)) => Data.Dec(a + b)
          case _ => Data.NA
        }
        case Min(_) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Int(a), Data.Int(b)) => Data.Int(a.min(b))
          case (Data.Number(a), Data.Number(b)) => Data.Dec(a.min(b))
          case (Data.Str(a), Data.Str(b)) => Data.Str(a) // TODO fix this
          case _ => Data.NA
        }
        case Max(_) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Int(a), Data.Int(b)) => Data.Int(a.max(b))
          case (Data.Number(a), Data.Number(b)) => Data.Dec(a.max(b))
          case (Data.Str(a), Data.Str(b)) => Data.Str(a) // TODO fix this
          case _ => Data.NA
        }
        case Avg(_) => ??? // TODO implement avg
        case Arbitrary(_) => (d1: Data, d2: Data) => d1
        case UnshiftArray(a) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Arr(a), Data.Arr(b)) => Data.Arr(a ++ b)
          case _ => Data.NA
        }
        case UnshiftMap(a1, a2) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Obj(a), Data.Obj(b)) => Data.Obj(a ++ b)
          case _ => Data.NA
        }

      }

      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, QScriptCore[T, ?], RDD[Data]] = {
        case qscript.Map(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                // TODO extract to a single method for compile-time efficiency
                freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.map(df))).point[Task]
            }
          )
        case Reduce(src, bucket, reducers, repair) =>
          val maybePartitioner: PlannerError \/ (Data => Data) =
            freeCataM(bucket)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          val extractFunc: ReduceFunc[Data => Data] => (Data => Data) = {
            case Count(a) => a >>> {
              case Data.NA => Data.Int(0)
              case _ => Data.Int(1)
            }
            case Sum(a) => a
            case Min(a) => a
            case Max(a) => a
            case Avg(a) => a
            case Arbitrary(a) => a
            case UnshiftArray(a) => a >>> ((d: Data) => Data.Arr(List(d)))
            case UnshiftMap(a1, a2) => ((d: Data) => a1(d) match {
              case Data.Str(k) => Data.Obj(ListMap(k -> a2(d)))
              case _ => Data.NA
            })
          }

          val maybeTransformers: PlannerError \/ List[Data => Data] =
            reducers.traverse(red => (red.traverse(freeCataM(_: FreeMap[T])(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)))).map(extractFunc))

          val reducersFuncs: List[(Data,Data) => Data] =
            reducers.map(reduceData)

          val maybeRepair: PlannerError \/ (Data => Data) =
            freeCataM(repair)(interpretM({
              case ReduceIndex(i) => ((x: Data) => x match {
                case Data.Arr(elems) => elems(i)
                case _ => Data.NA
              }).right
            }, CoreMap.change))

          def merge(a: Data, b: Data, f: List[(Data, Data) => Data]): Data = (a, b) match {
            case (Data.Arr(l1), Data.Arr(l2)) => Data.Arr(Zip[List].zipWith(f,(l1.zip(l2))) {
              case (f, (l1, l2)) => f(l1,l2)
            })
            case _ => Data.NA
          }

          StateT((sc: SparkContext) =>
            EitherT(((maybePartitioner |@| maybeTransformers |@| maybeRepair) {
              case (partitioner, trans, repair) =>
                src.map(d => (partitioner(d), Data.Arr(trans.map(_(d))) : Data))
                  .reduceByKey(merge(_,_, reducersFuncs))
                  .map {
                  case (k, v) => repair(v)
                }
            }).map((sc, _)).point[Task])
          )
        case Sort(src, bucket, order) =>
          unimplemented("sort")
        case Filter(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.filter{
                df >>> {
                  case Data.Bool(b) => b
                  case _ => false
                }
              })).point[Task]
            }
          )
        case Subset(src, from, sel, count) =>
          filterOut(fromFile, src, from, count,
            sel match {
              case Drop => (i: Index, c: Count) => i >= c
              case Take => (i: Index, c: Count) => i < c
              // TODO: Better sampling
              case Sample => (i: Index, c: Count) => i < c
            })

        case LeftShift(src, struct, repair) =>

          val structFunc: PlannerError \/ (Data => Data) =
            freeCataM(struct)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          def repairFunc: PlannerError \/ ((Data, Data) => Data) = {
            val dd: PlannerError \/ (Data => Data) =
              freeCataM(repair)(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data]({
                case LeftSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(0)
                  case _ => Data.NA
                }).right
                case RightSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(1)
                  case _ => Data.NA
                }).right
              }, CoreMap.change))

            dd.map(df => (l, r) => df(Data.Arr(List(l, r))))
          }

          StateT((sc: SparkContext) =>
            EitherT((structFunc ⊛ repairFunc)((df, rf) =>
              src.flatMap((input: Data) => df(input) match {
                case Data.Arr(list) => list.map(rf(input, _))
                case Data.Obj(m) => m.values.map(rf(input, _))
                case _ => List.empty[Data]
              })).map((sc, _)).point[Task]))

        case Union(src, lBranch, rBranch) =>
          val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
          val srcState = src.point[SparkState]

          (freeCataM(lBranch)(interpretM(κ(srcState), algebraM)) ⊛
            freeCataM(rBranch)(interpretM(κ(srcState), algebraM)))(_ ++ _)
        case Unreferenced() =>
          StateT((sc: SparkContext) => {
            EitherT((sc, sc.parallelize(List(Data.Null: Data))).right[PlannerError].point[Task])
          })
      }
    }

  implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, EquiJoin[T, ?], RDD[Data]] = {
        case EquiJoin(src, lBranch, rBranch, lKey, rKey, jt, combine) =>
          val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
          val srcState = src.point[SparkState]

          def genKey(kf: FreeMap[T]): SparkState[(Data => Data)] = EitherT(freeCataM(kf)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)).point[Task]).liftM[SparkStateT]
          
          val merger: SparkState[Data => Data] = 
            EitherT((freeCataM(combine)(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data]({
              case LeftSide => ((x: Data) => x match {
                case Data.Arr(elems) => elems(0)
                case _ => Data.NA
              }).right
              case RightSide => ((x: Data) => x match {
                case Data.Arr(elems) => elems(1)
                case _ => Data.NA
              }).right
            }, CoreMap.change))).point[Task]).liftM[SparkStateT]

          for {
            lk <- genKey(lKey)
            rk <- genKey(rKey)
            lRdd <- freeCataM(lBranch)(interpretM(κ(srcState), algebraM))
            rRdd <- freeCataM(rBranch)(interpretM(κ(srcState), algebraM))
            merge <- merger
          } yield {
            val klRdd = lRdd.map(d => (lk(d), d))
            val krRdd = rRdd.map(d => (rk(d), d))

            jt match {
              case Inner => klRdd.join(krRdd).map {
                case (k, (l, r)) => merge(Data.Arr(List(l, r)))
              }
              case LeftOuter => klRdd.leftOuterJoin(krRdd).map {
                case (k, (l, Some(r))) => merge(Data.Arr(List(l, r)))
                case (k, (l, None)) => merge(Data.Arr(List(l, Data.Null)))
              }
              case RightOuter => klRdd.rightOuterJoin(krRdd).map {
                case (k, (Some(l), r)) => merge(Data.Arr(List(l, r)))
                case (k, (None, r)) => merge(Data.Arr(List(Data.Null, r)))
              }
              case FullOuter => klRdd.fullOuterJoin(krRdd).map {
                case (k, (Some(l), Some(r))) => merge(Data.Arr(List(l, r)))
                case (k, (Some(l), None)) => merge(Data.Arr(List(l, Data.Null)))
                case (k, (None, Some(r))) => merge(Data.Arr(List(Data.Null, r)))
                case (k, (None, None)) => merge(Data.Arr(List(Data.Null, Data.Null)))
              }
            }
          }
      }
    }
  
  implicit def coproduct[T[_[_]]: Recursive: ShowT, F[_], G[_]](
    implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
      Planner.Aux[T, Coproduct[F, G, ?]] =
    new Planner[Coproduct[F, G, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }
}

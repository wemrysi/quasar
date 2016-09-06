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

import simulacrum.typeclass
import quasar.Predef._
import quasar.Data
import quasar.DataCodec
import quasar._
import quasar.Planner._
import quasar.fp._
import quasar.qscript._
import quasar.qscript.MapFuncs._
import quasar.fs._
import quasar.std.DateLib

import java.math.{BigDecimal => JBigDecimal}
// import org.threeten.bp.{LocalDate, LocalTime}
import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz.{Divide => _, _}, Scalaz._


@typeclass trait Planner[F[_]] {
  type IT[G[_]]

  def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], F, RDD[Data]]

}

object Planner {
  
  type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }
  
  // NB: Shouldn’t need this once we convert to paths.
  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
    new Planner[Const[DeadEnd, ?]] {
      type IT[G[_]] = T[G]
      
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], Const[DeadEnd, ?], RDD[Data]] =
        (qs: Const[DeadEnd, RDD[Data]]) =>
      StateT((sc: SparkContext) => {
        (sc, sc.parallelize(List(Data.Null: Data))).right[PlannerError]
      })
    }

  implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] =
    new Planner[Const[Read, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]) =
        (qs: Const[Read, RDD[Data]]) => {
          StateT((sc: SparkContext) => {
            val filePath = qs.getConst.path
            val rdd = fromFile(sc, filePath).map { raw =>
                DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
              }
            (sc, rdd).right[PlannerError]
          })
        }
    }
  
  implicit def sourcedPathable[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, SourcedPathable[T, ?]] =
    new Planner[SourcedPathable[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], SourcedPathable[T,?], RDD[Data]] = {
        case LeftShift(src, struct, repair) => ???
        case Union(src, lBranch, rBranch) => ???
      }
    }

  private def add(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a + b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a + BigDecimal(new JBigDecimal(b.bigInteger)))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(new JBigDecimal(a.bigInteger)) + b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.plus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.plus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.plus(b))
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.plus(b))
    case _ => Data.NA
  }

  private def subtract(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a - b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a - BigDecimal(new JBigDecimal(b.bigInteger)))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a - b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(new JBigDecimal(a.bigInteger)) - b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.minus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.minus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.minus(b))
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.minus(b))
    case _ => Data.NA
  }

  private def multiply(d1: Data, d2: Data): Data = (d1, d2)  match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a * b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a * BigDecimal(new JBigDecimal(b.bigInteger)))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a * b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(new JBigDecimal(a.bigInteger)) * b)
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => Data.NA
  }

  private def divide(d1: Data, d2: Data): Data = (d1, d2) match {
    // TODO missibin Temporal
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a / b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a / BigDecimal(new JBigDecimal(b.bigInteger)))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a / b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(new JBigDecimal(a.bigInteger)) / b)
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => Data.NA
  }

  // TODO: replace NA with something safer
  def change[T[_[_]]]: AlgebraM[PlannerError \/ ?, MapFunc[T, ?], Data => Data] = {
    case Length(f) => (f >>> {
      case Data.Str(v) => Data.Int(v.length)
      case Data.Arr(v) => Data.Int(v.size)
      case _ => Data.NA // this should never happen (see TODO above)
    } ).right
    case Date(f) => (f >>> {
      case Data.Str(v) => DateLib.parseDate(v).getOrElse(Data.NA)
      case _ => Data.NA 
    }).right
    case Time(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTime(v).getOrElse(Data.NA)
      case _ => Data.NA
    }).right
    case Timestamp(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTimestamp(v).getOrElse(Data.NA)
      case _ => Data.NA
    }).right
    case Interval(f) => (f >>> {
      case Data.Str(v) => DateLib.parseInterval(v).getOrElse(Data.NA)
      case _ => Data.NA
    }).right
    case TimeOfDay(f) => ??? // FIXME
    case ToTimestamp(f) => ??? // FIXME
    case Extract(f1, f2) => ??? // FIXME
    case Negate(f) => (f >>> {
      case Data.Bool(v) => Data.Bool(!v)
      case _ => Data.NA
    }).right
      // TODO not all possible pairs were matched, should they?
    case Add(f1, f2) => ((x: Data) => add(f1(x), f2(x))).right
    case Multiply(f1, f2) => ((x: Data) => multiply(f1(x), f2(x))).right
    case Subtract(f1, f2) => ((x: Data) => subtract(f1(x), f2(x))).right
    case Divide(f1, f2) => ((x: Data) => divide(f1(x), f2(x))).right
    case Modulo(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a % b)
      case _ => Data.NA
    }).right
    case ToString(f) => (f >>>  {
      case Data.Null => Data.Str("null")
      case d: Data.Str => d
      case Data.Bool(v) => Data.Str(v.toString)
      case Data.Dec(v) => Data.Str(v.toString)
      case Data.Int(v) => Data.Str(v.toString)
      // TODO how to handle obj and collections
      case Data.Obj(v) => ???
      case Data.Arr(v) => ???
      case Data.Set(v) => ???
      case Data.Timestamp(v) => Data.Str(v.toString)
      case Data.Date(v) => Data.Str(v.toString)
      case Data.Time(v) => Data.Str(v.toString)
      case Data.Interval(v) => Data.Str(v.toString)
      case Data.Binary(v) => Data.Str(v.toList.mkString(""))
      case Data.Id(s) => Data.Str(s)
      // TODO fails on d: Data.NA
      case d => d
    }).right
    // case Add(a, b) => x => a(x) - b(x)
    case _ => InternalError("not implemented").left
  }

  implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], QScriptCore[T, ?], RDD[Data]] = {
        case qscript.Map(src, f) =>
          StateT((sc: SparkContext) => freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), change)).map(df => (sc, src.map(df))))
        case Reduce(src, bucket, reducers, repair) =>
          ???
        case Sort(src, bucket, order) =>
          ???
        case Filter(src, f) =>
          ???
        case Take(src, from, count) =>
          ???
        case Drop(src, from, count) =>
          ???
      }
    }
  
  implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], EquiJoin[T, ?], RDD[Data]] = ???      
    }
  
  // TODO: Remove this instance
  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
    new Planner[ThetaJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], ThetaJoin[T, ?], RDD[Data]] = ???
    }
  
  // TODO: Remove this instance
  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
    new Planner[ProjectBucket[T, ?]] {
      type IT[G[_]] = T[G]
            def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], ProjectBucket[T, ?], RDD[Data]] = ???
    }
  
  implicit def coproduct[T[_[_]]: Recursive: ShowT, F[_], G[_]](
    implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
      Planner.Aux[T, Coproduct[F, G, ?]] =
    new Planner[Coproduct[F, G, ?]] {
      type IT[G[_]] = T[G]
      def plan(fromFile: (SparkContext, AFile) => RDD[String]): AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }
}

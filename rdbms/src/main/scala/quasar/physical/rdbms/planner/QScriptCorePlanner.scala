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

package quasar.physical.rdbms.planner

import slamdata.Predef._
import quasar.common.SortDir
import quasar.fp.ski._
import quasar.{NameGenerator, qscript}
import quasar.Planner.PlannerErrorME
import Planner._
import sql.SqlExpr._
import sql.{SqlExpr, _}
import sql.SqlExpr.Select._
import quasar.qscript.{FreeMap, MapFunc, MapFuncsCore, QScriptCore, QScriptTotal, ReduceFunc, ReduceFuncs}
import quasar.qscript.{MapFuncCore => MFC}
import quasar.qscript.{MapFuncDerived => MFD}
import quasar.physical.rdbms.planner.sql.Metas._
import ReduceFuncs.Arbitrary

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._
import Scalaz._

class QScriptCorePlanner[T[_[_]]: BirecursiveT: ShowT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  def * : T[SqlExpr] = AllCols[T[SqlExpr]]().embed

  private def processFreeMap(f: FreeMap[T],
                     alias: SqlExpr[T[SqlExpr]]): F[T[SqlExpr]] =
    f.cataM(interpretM(κ(alias.embed.η[F]), mapFuncPlanner.plan))

  private def take(fromExpr: F[T[SqlExpr]], countExpr: F[T[SqlExpr]]): F[T[SqlExpr]] =
    (fromExpr |@| countExpr)(Limit(_, _).embed)

  private def drop(fromExpr: F[T[SqlExpr]], countExpr: F[T[SqlExpr]]): F[T[SqlExpr]] =
    (fromExpr |@| countExpr)(Offset(_, _).embed)

  private def compile(expr: qscript.FreeQS[T], src: T[SqlExpr]): F[T[SqlExpr]] = {
    val compiler = Planner[T, F, QScriptTotal[T, ?]].plan
    expr.cataM(interpretM(κ(src.point[F]), compiler))
  }

  val unref: T[SqlExpr] = SqlExpr.Unreferenced[T[SqlExpr]]().embed

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(`unref`, f) =>
      processFreeMap(f, SqlExpr.Null[T[SqlExpr]])
    case qscript.Map(src, f) =>
      for {
        fromAlias <- genIdWithMeta[T[SqlExpr], F](deriveMeta(src))
        selection <- processFreeMap(f, fromAlias)
          .map(_.project match {
            case SqlExpr.Id(_, _) => *
            case other => other.embed
          })
      } yield {
        Select(
          Selection(selection, none, deriveMeta(src)),
          From(src, fromAlias),
          join = none,
          filter = none,
          groupBy = none,
          orderBy = nil
        ).embed
      }
    case qscript.Sort(src, bucket, order) =>
      def createOrderBy(id: SqlExpr.Id[T[SqlExpr]]):
      ((FreeMap[T], SortDir)) => F[OrderBy[T[SqlExpr]]] = {
        case (qs, dir) =>
          processFreeMap(qs, id).map { expr =>
            OrderBy(expr, dir)
          }
      }
      for {
        fromAlias <- genId[T[SqlExpr], F]
        orderByExprs <- order.traverse(createOrderBy(fromAlias))
        bucketExprs <- bucket.map((_, orderByExprs.head.sortDir)).traverse(createOrderBy(fromAlias))
      }
        yield {
          Select(
            Selection(*, none),
            From(src, fromAlias),
            join = none,
            filter = none,
            groupBy = none,
            orderBy = bucketExprs ++ orderByExprs.toList
          ).embed
        }
    case qscript.Subset(src, from, sel, count) =>
      val fromExpr: F[T[SqlExpr]]  = compile(from, src)
      val countExpr: F[T[SqlExpr]] = compile(count, src)

      sel match {
        case qscript.Drop   => drop(fromExpr, countExpr)
        case qscript.Take   => take(fromExpr, countExpr)
        case qscript.Sample => take(fromExpr, countExpr) // TODO needs better sampling (which connector doesn't?)
      }

    case qscript.Unreferenced() => unref.point[F]

    case qscript.Filter(src, f) =>
      src.project match {
        case s@Select(_, From(_, initialFromAlias), _, initialFilter, _,  _) =>
          val injectedFilterExpr = processFreeMap(f, initialFromAlias)
          injectedFilterExpr.map { fe =>
            val finalFilterExpr = initialFilter.map(i => And[T[SqlExpr]](i.v, fe).embed).getOrElse(fe)
            s.copy(filter = Some(Filter[T[SqlExpr]](finalFilterExpr))).embed
          }
        case other =>
          for {
            fromAlias <- genIdWithMeta[T[SqlExpr], F](deriveMeta(src))
            filterExp <- processFreeMap(f, fromAlias)
          } yield {
          Select(
            Selection(*, none, deriveMeta(src)),
            From(src, fromAlias),
            join = none,
            Some(Filter(filterExp)),
            groupBy = none,
            orderBy = nil
          ).embed
      }
    }

    case qUnion@qscript.Union(src, left, right) =>
      (compile(left, src) |@| compile(right, src))(Union(_,_).embed)

    case DisctinctPattern(qscript.Reduce(src, _, _, _)) => for {
      alias <- genId[T[SqlExpr], F]
    } yield Select(
      Selection(Distinct[T[SqlExpr]](*).embed, none),
      From(src, alias),
      filter = none,
      join = none,
      groupBy = none,
      orderBy = nil
    ).embed

    case reduce@qscript.Reduce(src, bucket, reducers, repair) => for {
      alias <- genIdWithMeta[T[SqlExpr], F](Branch(
        (_: String) => (Dot, deriveMeta(src)), s"(Dot, ${deriveMeta(src).shows})"))
      gbs   <- bucket.traverse(processFreeMap(_, alias))
      rds   <- reducers.traverse(_.traverse(processFreeMap(_, alias)) >>=
        reduceFuncPlanner[T, F].plan)
      rep <- repair.cataM(interpretM(
        _.idx.fold(
          idx => notImplemented("Reduce repair with left index, waiting for a test case", this): F[T[SqlExpr]],
          idx => rds(idx).point[F]
        ),
        Planner.mapFuncPlanner[T, F].plan)
      )
    } yield Select(
      Selection(rep, none),
      From(src, alias),
      join = none,
      filter = none,
      groupBy = GroupBy(gbs).some,
      orderBy = nil
    ).embed

    case other =>
      notImplemented(s"QScriptCore: $other", this)
  }

  object DisctinctPattern {

    def isHoleOrGuardedHole(fm: FreeMap[T]): Boolean =
      fm.cata(interpret(κ(true), (copro: MapFunc[T, Boolean]) =>
        copro.fold(
          new ~>[MFC[T, ?], Option] {
            def apply[A](fa: MFC[T, A]): Option[A] = {
              fa match {
                case MapFuncsCore.Guard(_, _, a, _) => a.some
                case _ => none
              }
            }
          },
          new ~>[MFD[T, ?], Option] {
            def apply[A](fa: MFD[T, A]): Option[A] = none
          }
        ).exists(ι)
      ))

    object BucketWithSingleHole {
      def unapply(fms: List[FreeMap[T]]): Boolean =
        fms.headOption.exists(isHoleOrGuardedHole)
    }

    object ReducersWithSingleArbitraryHole {
      def unapply(fms: List[ReduceFunc[FreeMap[T]]]): Boolean =
        fms.headOption.exists {
          case Arbitrary(fm) => isHoleOrGuardedHole(fm)
          case _ => false
        }
    }

    def unapply[A](qs: QScriptCore[T, A]): Option[qscript.Reduce[T, A]] = qs match {
      case reduce@qscript.Reduce(_, BucketWithSingleHole(), ReducersWithSingleArbitraryHole(), _) => reduce.some
      case _ => none
    }
  }
}

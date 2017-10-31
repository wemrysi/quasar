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

import quasar.{
  ejson,
  BinaryFunc,
  Data,
  Mapping,
  NameGenerator,
  Reduction,
  TernaryFunc,
  UnaryFunc
}
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.contrib.pathy.mkAbsolute
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.EJson
import quasar.frontend.{logicalplan => lp}
import quasar.std.{IdentityLib, SetLib, StructuralLib}
import quasar.qscript.{Hole, MapFunc, MapFuncsCore, MFC, ReduceFunc, SrcHole, TTypes}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef.{Map => SMap, _}

import matryoshka.{AlgebraM, BirecursiveT}
import matryoshka.implicits._
import matryoshka.patterns.{interpretM, CoEnv}
import pathy.Path.{rootDir, Sandboxed}
import scalaz.{\/, Free, Inject, Monad, NonEmptyList => NEL}
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import shapeless.Sized

sealed abstract class ReadLP[
    T[_[_]]: BirecursiveT,
    F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator]
    extends TTypes[T] {

  private val IC = Inject[MapFuncCore, MapFunc]
  private val ID = Inject[MapFuncDerived, MapFunc]

  private val IdIndex = 0
  private val ValueIndex = 1

  def apply(plan: T[lp.LogicalPlan]): F[QSUGraph[T]] =
    plan.cataM(transform)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val transform: AlgebraM[F, lp.LogicalPlan, QSUGraph[T]] = {
    case lp.Read(path) =>
      val afile = mkAbsolute(rootDir[Sandboxed], path)    // TODO this is almost certainly wrong
      withName(QSU.Read[T, Symbol](afile))

    case lp.Constant(data) =>
      val back = fromData(data).fold[PlannerError \/ QSU[T, Symbol]](
        {
          case Data.NA =>
            QSU.Nullary(IC(MapFuncsCore.Undefined[T, Symbol]())).right

          case d =>
            NonRepresentableData(d).left
        },
        { x: T[EJson] => QSU.Constant[T, Symbol](x).right })

      MonadError_[F, PlannerError].unattempt(back.point[F]).flatMap(withName)

    case lp.InvokeUnapply(StructuralLib.FlattenMap, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.FlattenMap)).map(_ :++ a)

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenMapKeys, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.FlattenMap)).map(_ :++ a)

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenArray, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.FlattenArray)).map(_ :++ a)

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenArrayIndices, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.FlattenArray)).map(_ :++ a)

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftMap, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.ShiftMap)).map(_ :++ a)

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftMapKeys, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.ShiftMap)).map(_ :++ a)

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftArray, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.ShiftArray)).map(_ :++ a)

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftArrayIndices, Sized(a)) =>
      val transpose =
        withName(QSU.Transpose[T, Symbol](a.root, QSU.Rotation.ShiftArray)).map(_ :++ a)

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(SetLib.GroupBy, Sized(a, b)) =>
      withName(QSU.GroupBy[T, Symbol](a.root, b.root)).map(_ :++ a :++ b)

    case lp.InvokeUnapply(IdentityLib.Squash, Sized(a)) =>
      withName(QSU.DimEdit[T, Symbol](a.root, QSU.DTrans.Squash())).map(_ :++ a)

    case lp.InvokeUnapply(SetLib.Filter, Sized(a, b)) =>
      withName(QSU.LPFilter[T, Symbol](a.root, b.root)).map(_ :++ a :++ b)

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Reduction =>
      val translated = ReduceFunc.translateUnaryReduction[Unit](func)(())
      withName(QSU.LPReduce[T, Symbol](a.root, translated)).map(_ :++ a)

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Reduction => ???

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Mapping =>
      val translated =
        MapFunc.translateUnaryMapping[T, MapFunc, Free[MapFunc, Hole]].apply(func)(
          Free.pure[MapFunc, Hole](SrcHole))

      val node = QSU.Map[T, Symbol](a.root, Free.roll[MapFunc, Hole](translated))
      withName(node).map(_ :++ a)

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Mapping =>
      val translated =
        MapFunc.translateBinaryMapping[T, MapFunc, Int].apply(func)(0, 1)

      val node = QSU.AutoJoin[T, Symbol](NEL(a.root, b.root), translated)
      withName(node).map(_ :++ a :++ b)

    case lp.InvokeUnapply(func: TernaryFunc, Sized(a, b, c)) if func.effect === Mapping =>
      val translated =
        MapFunc.translateTernaryMapping[T, MapFunc, Int].apply(func)(0, 1, 2)

      val node = QSU.AutoJoin[T, Symbol](NEL(a.root, b.root, c.root), translated)
      withName(node).map(_ :++ a :++ b :++ c)

    case lp.InvokeUnapply(func, values) => ???

    case lp.TemporalTrunc(part, src) =>
      val node = QSU.Map[T, Symbol](
        src.root,
        Free.roll[MapFunc, Hole](
          MFC(MapFuncsCore.TemporalTrunc(part, Free.pure[MapFunc, Hole](SrcHole)))))

      withName(node).map(_ :++ src)

    case lp.Typecheck(expr, tpe, cont, fallback) =>
      val translated = IC(MapFuncsCore.Guard[T, Int](0, tpe, 1, 2))

      val node = QSU.AutoJoin[T, Symbol](NEL(expr.root, cont.root, fallback.root), translated)
      withName(node).map(_ :++ expr :++ cont :++ fallback)

    case lp.JoinSideName(name) =>
      withName(QSU.JoinSideRef[T, Symbol](name))

    case lp.Join(left, right, tpe, lp.JoinCondition(leftN, rightN, cond)) =>
      val node = QSU.LPJoin[T, Symbol](
        left.root,
        right.root,
        cond.root,
        tpe,
        leftN,
        rightN)

      withName(node).map(_ :++ left :++ right :++ cond)

    case lp.Free(name) =>
      QSUGraph[T](name, SMap()).point[F]

    case lp.Let(name, form, in) =>
      (form.rename(form.root, name) ++: in).point[F]

    case lp.Sort(src, order) =>
      val node = QSU.Sort[T, Symbol](src.root, order.map(_.leftMap(_.root)))
      val graphs = src <:: order.map(_._1)

      withName(node).map(g => graphs.foldLeft(g)(_ :++ _))
  }

  private def projectConstIdx(idx: Int)(parent: QSUGraph[T]): F[QSUGraph[T]] = {
    for {
      idxG <- withName(QSU.Constant[T, Symbol](
        ejson.ExtEJson(ejson.Int[T[EJson]](idx)).embed))
      func = IC(MapFuncsCore.ProjectIndex[T, Int](0, 1))
      nodeG <- withName(QSU.AutoJoin[T, Symbol](NEL(parent.root, idxG.root), func))
    } yield nodeG :++ idxG :++ parent
  }

  private def withName(node: QSU[T, Symbol]): F[QSUGraph[T]] = {
    for {
      name <- NameGenerator[F].prefixedName("qsu")
      sym = Symbol(name)
    } yield QSUGraph(root = sym, SMap(sym -> node))
  }

  private def fromData(data: Data): Data \/ T[EJson] = {
    data.hyloM[Data \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[Data \/ ?, EJson, Data, T[EJson]](
        _.left,
        _.embed.right),
      Data.toEJson[EJson].apply(_).right)
  }
}

object ReadLP {
  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator]: ReadLP[T, F] =
    new ReadLP[T, F] {}
}

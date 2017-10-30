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
import quasar.contrib.scalaz.{MonadError_, MonadState_}
import quasar.ejson.EJson
import quasar.frontend.{logicalplan => lp}
import quasar.std.{IdentityLib, SetLib, StructuralLib}
import quasar.qscript.{
  Drop,
  Hole,
  MapFunc,
  MapFuncsCore,
  MFC,
  ReduceFunc,
  Sample,
  SrcHole,
  Take
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef.{Map => SMap, _}

import matryoshka.{AlgebraM, BirecursiveT}
import matryoshka.implicits._
import matryoshka.patterns.{interpretM, CoEnv}
import pathy.Path.{rootDir, Sandboxed}
import scalaz.{\/, Forall, Free, Inject, Monad, NonEmptyList => NEL, StateT}
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
    extends QSUTTypes[T] {

  type QSU[A] = QScriptUniform[A]
  type G[A] = StateT[F, SMap[QSU[Symbol], Symbol], A]

  private val MS = MonadState_[G, SMap[QSU[Symbol], Symbol]]

  private val IC = Inject[MapFuncCore, MapFunc]
  private val ID = Inject[MapFuncDerived, MapFunc]

  private val IdIndex = 0
  private val ValueIndex = 1

  def apply(plan: T[lp.LogicalPlan]): F[QSUGraph] =
    plan.cataM(transform).eval(SMap())

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val transform: AlgebraM[G, lp.LogicalPlan, QSUGraph] = {
    case lp.Read(path) =>
      val afile = mkAbsolute(rootDir[Sandboxed], path)

      for {
        read <- withName(QSU.Read[T, Symbol](afile))
        back <- extend1(read)(QSU.Transpose[T, Symbol](_, QSU.Rotation.ShiftMap))
      } yield back

    case lp.Constant(data) =>
      val back = fromData(data).fold[PlannerError \/ QSU[Symbol]](
        {
          case Data.NA =>
            QSU.Nullary[T, Symbol](Forall(_(IC(MapFuncsCore.Undefined())))).right

          case d =>
            NonRepresentableData(d).left
        },
        { x: T[EJson] => QSU.Constant[T, Symbol](x).right })

      MonadError_[G, PlannerError].unattempt(back.point[G]).flatMap(withName)

    case lp.InvokeUnapply(StructuralLib.FlattenMap, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.FlattenMap))

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenMapKeys, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.FlattenMap))

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenArray, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.FlattenArray))

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.FlattenArrayIndices, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.FlattenArray))

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftMap, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.ShiftMap))

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftMapKeys, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.ShiftMap))

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftArray, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.ShiftArray))

      transpose >>= projectConstIdx(ValueIndex)

    case lp.InvokeUnapply(StructuralLib.ShiftArrayIndices, Sized(a)) =>
      val transpose =
        extend1(a)(QSU.Transpose[T, Symbol](_, QSU.Rotation.ShiftArray))

      transpose >>= projectConstIdx(IdIndex)

    case lp.InvokeUnapply(SetLib.GroupBy, Sized(a, b)) =>
      extend2(a, b)(QSU.GroupBy[T, Symbol](_, _))

    case lp.InvokeUnapply(IdentityLib.Squash, Sized(a)) =>
      extend1(a)(QSU.DimEdit[T, Symbol](_, QSU.DTrans.Squash()))

    case lp.InvokeUnapply(SetLib.Filter, Sized(a, b)) =>
      extend2(a, b)(QSU.LPFilter[T, Symbol](_, _))

    case lp.InvokeUnapply(SetLib.Sample, Sized(a, b)) =>
      extend2(a, b)(QSU.Subset[T, Symbol](_, Sample, _))

    case lp.InvokeUnapply(SetLib.Take, Sized(a, b)) =>
      extend2(a, b)(QSU.Subset[T, Symbol](_, Take, _))

    case lp.InvokeUnapply(SetLib.Drop, Sized(a, b)) =>
      extend2(a, b)(QSU.Subset[T, Symbol](_, Drop, _))

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Reduction =>
      val translated = ReduceFunc.translateUnaryReduction[Unit](func)(())
      extend1(a)(QSU.LPReduce[T, Symbol](_, translated))

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Reduction => ???

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Mapping =>
      val translated =
        MapFunc.translateUnaryMapping[T, MapFunc, Free[MapFunc, Hole]].apply(func)(
          Free.pure[MapFunc, Hole](SrcHole))

      extend1(a)(QSU.Map[T, Symbol](_, Free.roll[MapFunc, Hole](translated)))

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Mapping =>
      autoJoin2(a, b)(MapFunc.translateBinaryMapping[T, MapFunc, Int].apply(func))

    case lp.InvokeUnapply(func: TernaryFunc, Sized(a, b, c)) if func.effect === Mapping =>
      autoJoin3(a, b, c)(MapFunc.translateTernaryMapping[T, MapFunc, Int].apply(func))

    case lp.InvokeUnapply(func, values) => ???

    case lp.TemporalTrunc(part, src) =>
      extend1(src)(
        QSU.Map[T, Symbol](
          _,
          Free.roll[MapFunc, Hole](
            MFC(MapFuncsCore.TemporalTrunc(part, Free.pure[MapFunc, Hole](SrcHole))))))

    case lp.Typecheck(expr, tpe, cont, fallback) =>
      autoJoin3(expr, cont, fallback)((e, c, f) => IC(MapFuncsCore.Guard[T, Int](e, tpe, c, f)))

    case lp.JoinSideName(name) =>
      withName(QSU.JoinSideRef[T, Symbol](name))

    case lp.Join(left, right, tpe, lp.JoinCondition(leftN, rightN, cond)) =>
      extend3(left, right, cond)(
        QSU.LPJoin[T, Symbol](_, _, _, tpe, leftN, rightN))

    case lp.Free(name) =>
      QSUGraph[T](name, SMap()).point[G]

    case lp.Let(name, form, in) =>
      for {
        reverse <- MS.get
        _ <- MS.put(reverse.updated(form.vertices(form.root), name))
      } yield {
        val root2 = name

        val vertices2 =
          form.vertices.get(form.root).map(node => form.vertices - form.root + (name -> node)).getOrElse(form.vertices)

        QSUGraph(root2, vertices2)
      }

    case lp.Sort(src, order) =>
      val node = QSU.Sort[T, Symbol](src.root, order.map(_.leftMap(_.root)))
      val graphs = src <:: order.map(_._1)

      withName(node).map(g => graphs.foldLeft(g)(_ :++ _))
  }

  private def projectConstIdx(idx: Int)(parent: QSUGraph): G[QSUGraph] = {
    for {
      idxG <- withName(
        QSU.Constant[T, Symbol](
          ejson.ExtEJson(ejson.Int[T[EJson]](idx)).embed))

      back <- autoJoin2(parent, idxG)((p, i) => IC(MapFuncsCore.ProjectIndex[T, Int](p, i)))
    } yield back
  }

  private def autoJoin2(e1: QSUGraph, e2: QSUGraph)(
      constr: (Int, Int) => MapFunc[Int]): G[QSUGraph] =
    extend2(e1, e2)((e1, e2) => QSU.AutoJoin[T, Symbol](NEL(e1, e2), constr(0, 1)))

  private def autoJoin3(e1: QSUGraph, e2: QSUGraph, e3: QSUGraph)(
      constr: (Int, Int, Int) => MapFunc[Int]): G[QSUGraph] =
    extend3(e1, e2, e3)((e1, e2, e3) => QSU.AutoJoin[T, Symbol](NEL(e1, e2, e3), constr(0, 1, 2)))

  private def extend1(parent: QSUGraph)(
      constr: Symbol => QSU[Symbol]): G[QSUGraph] =
    withName(constr(parent.root)).map(_ :++ parent)

  private def extend2(parent1: QSUGraph, parent2: QSUGraph)(
      constr: (Symbol, Symbol) => QSU[Symbol]): G[QSUGraph] =
    withName(constr(parent1.root, parent2.root)).map(_ :++ parent1 :++ parent2)

  private def extend3(parent1: QSUGraph, parent2: QSUGraph, parent3: QSUGraph)(
      constr: (Symbol, Symbol, Symbol) => QSU[Symbol]): G[QSUGraph] =
    withName(constr(parent1.root, parent2.root, parent3.root)).map(_ :++ parent1 :++ parent2 :++ parent3)

  private def withName(node: QSU[Symbol]): G[QSUGraph] = {
    for {
      reverse <- MS.get

      back <- reverse.get(node) match {
        case Some(sym) =>
          QSUGraph[T](root = sym, SMap()).point[G]

        case None =>
          for {
            name <- NameGenerator[G].prefixedName("qsu")
            sym = Symbol(name)
            _ <- MS.put(reverse + (node -> sym))
          } yield QSUGraph[T](root = sym, SMap(sym -> node))
      }
    } yield back
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

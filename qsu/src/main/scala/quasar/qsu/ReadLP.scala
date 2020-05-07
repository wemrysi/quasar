/*
 * Copyright 2020 Precog Data
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

package quasar.qsu

import slamdata.Predef.{Map => SMap, _}
import quasar.{
  BinaryFunc,
  IdStatus,
  Mapping,
  NullaryFunc,
  Reduction,
  TernaryFunc,
  UnaryFunc
}
import quasar.common.data.Data
import quasar.common.effect.NameGenerator
import quasar.contrib.cats.stateT._
import quasar.contrib.pathy.mkAbsolute
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.EJson
import quasar.fp._
import quasar.contrib.iota._
import quasar.frontend.{logicalplan => lp}
import quasar.std.{IdentityLib, SetLib, StructuralLib}
import quasar.qscript.{
  Center,
  Drop,
  Hole,
  JoinSide,
  JoinSide3,
  LeftSide,
  LeftSide3,
  MapFunc,
  MapFuncsCore,
  MonadPlannerErr,
  PlannerError,
  ReduceFunc,
  RightSide,
  RightSide3,
  Sample,
  SrcHole,
  Take
}
import quasar.qscript.PlannerError.NonRepresentableData
import quasar.qsu.{QScriptUniform => QSU}

import cats.data.StateT

import iotaz.CopK

import matryoshka.{AlgebraM, BirecursiveT}
import matryoshka.implicits._
import matryoshka.patterns.{interpretM, CoEnv}

import pathy.Path.{rootDir, Sandboxed}

import scalaz.{\/, Free, Monad, Scalaz}, Scalaz._   // monad/traverse conflict

import shapeless.Sized

import shims.{monadToCats, monadToScalaz}

final class ReadLP[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {

  private type QSU[A] = QScriptUniform[A]
  private type LetS = SMap[Symbol, Symbol]

  private val IC = CopK.Inject[MapFuncCore, MapFunc]
  private val ID = CopK.Inject[MapFuncDerived, MapFunc]

  def apply[
      F[_]: Monad: MonadPlannerErr: NameGenerator](
      plan: T[lp.LogicalPlan]): F[QSUGraph] =
    cataWithLetM[StateT[StateT[F, RevIdx, ?], LetS, ?]](
      plan,
      readLPƒ[StateT[StateT[F, RevIdx, ?], LetS, ?]]).runA(SMap()).runA(SMap())

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def cataWithLetM[G[_]: Monad](
      plan: T[lp.LogicalPlan],
      alg: AlgebraM[G, lp.LogicalPlan, QSUGraph])(
      implicit LetS: MonadState_[G, LetS]): G[QSUGraph] = {

    plan.project match {
      case lp.Let(name, form, in) =>
        for {
          formA <- cataWithLetM[G](form, alg)
          _ <- LetS.modify(_ + (name -> formA.root))
          inA <- cataWithLetM[G](in, alg)
          _ <- LetS.modify(_ - name)    // don't assume LP compiler generates globally unique names
          back <- alg(lp.Let(name, formA, inA))
        } yield back

      case lpr =>
        lpr.traverse(cataWithLetM[G](_, alg)).flatMap(alg)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def readLPƒ[
      G[_]: Monad: MonadPlannerErr: NameGenerator](
      implicit
        MS: MonadState_[G, RevIdx],
        LetS: MonadState_[G, LetS])
      : AlgebraM[G, lp.LogicalPlan, QSUGraph] = {

    case lp.Read(path) =>
      withName[G](QSU.Read[T, Symbol](
        mkAbsolute(rootDir[Sandboxed], path),
        IdStatus.ExcludeId)) // `IdStatus` is updated in `ReifyIdentities`, when necessary

    case lp.Constant(data) =>
      val back = fromData(data).fold[PlannerError \/ MapFunc[Hole]](
        {
          case Data.NA =>
            IC(MapFuncsCore.Undefined()).right

          case d =>
            NonRepresentableData(d).left
        },
        { ejson: T[EJson] => IC(MapFuncsCore.Constant(ejson)).right })

      MonadPlannerErr[G]
        .unattempt(back.point[G])
        .flatMap(nullary[G])

    case lp.InvokeUnapply(StructuralLib.FlattenMap, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Values, QSU.Rotation.FlattenMap))

    case lp.InvokeUnapply(StructuralLib.FlattenMapKeys, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Identities, QSU.Rotation.FlattenMap))

    case lp.InvokeUnapply(StructuralLib.FlattenArray, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Values, QSU.Rotation.FlattenArray))

    case lp.InvokeUnapply(StructuralLib.FlattenArrayIndices, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Identities, QSU.Rotation.FlattenArray))

    case lp.InvokeUnapply(StructuralLib.ShiftMap, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Values, QSU.Rotation.ShiftMap))

    case lp.InvokeUnapply(StructuralLib.ShiftMapKeys, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Identities, QSU.Rotation.ShiftMap))

    case lp.InvokeUnapply(StructuralLib.ShiftArray, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Values, QSU.Rotation.ShiftArray))

    case lp.InvokeUnapply(StructuralLib.ShiftArrayIndices, Sized(a)) =>
      extend1[G](a)(QSU.Transpose[T, Symbol](_, QSU.Retain.Identities, QSU.Rotation.ShiftArray))

    case lp.InvokeUnapply(SetLib.GroupBy, Sized(a, b)) =>
      extend2[G](a, b)(QSU.GroupBy[T, Symbol](_, _))

    case lp.InvokeUnapply(IdentityLib.Squash, Sized(a)) =>
      extend1[G](a)(QSU.DimEdit[T, Symbol](_, QSU.DTrans.Squash()))

    case lp.InvokeUnapply(SetLib.Filter, Sized(a, b)) =>
      extend2[G](a, b)(QSU.LPFilter[T, Symbol](_, _))

    case lp.InvokeUnapply(SetLib.Sample, Sized(a, b)) =>
      extend2[G](a, b)(QSU.Subset[T, Symbol](_, Sample, _))

    case lp.InvokeUnapply(SetLib.Take, Sized(a, b)) =>
      extend2[G](a, b)(QSU.Subset[T, Symbol](_, Take, _))

    case lp.InvokeUnapply(SetLib.Drop, Sized(a, b)) =>
      extend2[G](a, b)(QSU.Subset[T, Symbol](_, Drop, _))

    case lp.InvokeUnapply(SetLib.Union, Sized(a, b)) =>
      extend2[G](a, b)(QSU.Union[T, Symbol](_, _))

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Reduction =>
      val translated = ReduceFunc.translateUnaryReduction[Unit](func)(())
      extend1[G](a)(QSU.LPReduce[T, Symbol](_, translated))

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Reduction => ???

    case lp.InvokeUnapply(func: NullaryFunc, Sized()) if func.effect === Mapping =>
      val translated: MapFunc[Hole] =
        MapFunc.translateNullaryMapping[T, MapFunc, Hole].apply(func)

      nullary[G](translated)

    case lp.InvokeUnapply(func: UnaryFunc, Sized(a)) if func.effect === Mapping =>
      val translated =
        MapFunc.translateUnaryMapping[T, MapFunc, Hole].apply(func)(SrcHole)

      extend1[G](a)(QSU.Unary[T, Symbol](_, translated))

    case lp.InvokeUnapply(func: BinaryFunc, Sized(a, b)) if func.effect === Mapping =>
      autoJoin2[G](a, b)(MapFunc.translateBinaryMapping[T, MapFunc, JoinSide].apply(func))

    case lp.InvokeUnapply(func: TernaryFunc, Sized(a, b, c)) if func.effect === Mapping =>
      autoJoin3[G](a, b, c)(MapFunc.translateTernaryMapping[T, MapFunc, JoinSide3].apply(func))

    case lp.InvokeUnapply(func, values) =>
      println(s"hit this case with $func and $values")
      ???

    case lp.TemporalTrunc(part, src) =>
      extend1[G](src)(
        QSU.Unary[T, Symbol](
          _,
          IC(MapFuncsCore.TemporalTrunc(part, SrcHole))))

    case lp.JoinSideName(name) =>
      withName[G](QSU.JoinSideRef[T, Symbol](name))

    case lp.Join(left, right, tpe, lp.JoinCondition(leftN, rightN, cond)) =>
      extend3[G](left, right, cond)(
        QSU.LPJoin[T, Symbol](_, _, _, tpe, leftN, rightN))

    case lp.Free(name) =>
      LetS.get map { lets =>
        QSUGraph[T](lets(name), SMap())
      }

    // we trust cataWithLetM to deal with renaming things
    case lp.Let(_, form, in) => (in :++ form).point[G]

    case lp.Sort(src, order) =>
      val node = QSU.LPSort[T, Symbol](src.root, order.map(_.leftMap(_.root)))
      val graphs = src <:: order.map(_._1)

      withName[G](node).map(g => graphs.foldLeft(g)(_ :++ _))
  }

  private def nullary[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](func: MapFunc[Hole])
      : G[QSUGraph] =
    for {
      source <- withName[G](QSU.Unreferenced[T, Symbol]())
      back <- extend1[G](source)(QSU.Unary[T, Symbol](_, func))
    } yield back

  private def autoJoin2[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      e1: QSUGraph, e2: QSUGraph)(
      constr: (JoinSide, JoinSide) => MapFunc[JoinSide]): G[QSUGraph] =
    extend2[G](e1, e2)((e1, e2) =>
      QSU.AutoJoin2[T, Symbol](e1, e2, Free.liftF(constr(LeftSide, RightSide))))

  private def autoJoin3[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      e1: QSUGraph, e2: QSUGraph, e3: QSUGraph)(
      constr: (JoinSide3, JoinSide3, JoinSide3) => MapFunc[JoinSide3]): G[QSUGraph] =
    extend3[G](e1, e2, e3)((e1, e2, e3) =>
      QSU.AutoJoin3[T, Symbol](e1, e2, e3, Free.liftF(constr(LeftSide3, Center, RightSide3))))

  private def extend1[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      parent: QSUGraph)(
      constr: Symbol => QSU[Symbol]): G[QSUGraph] =
    withName[G](constr(parent.root)).map(_ :++ parent)

  private def extend2[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      parent1: QSUGraph, parent2: QSUGraph)(
      constr: (Symbol, Symbol) => QSU[Symbol]): G[QSUGraph] =
    withName[G](constr(parent1.root, parent2.root)).map(_ :++ parent1 :++ parent2)

  private def extend3[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      parent1: QSUGraph, parent2: QSUGraph, parent3: QSUGraph)(
      constr: (Symbol, Symbol, Symbol) => QSU[Symbol]): G[QSUGraph] =
    withName[G](constr(parent1.root, parent2.root, parent3.root)).map(_ :++ parent1 :++ parent2 :++ parent3)

  private def withName[G[_]: Monad: NameGenerator: RevIdxM](node: QScriptUniform[Symbol]): G[QSUGraph] =
    QSUGraph.withName[T, G]("rlp")(node)

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
      F[_]: Monad: MonadPlannerErr: NameGenerator]
      (plan: T[lp.LogicalPlan])
      : F[QSUGraph[T]] =
    taggedInternalError("ReadLP", new ReadLP[T].apply[F](plan))
}

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

package quasar.qscript

import quasar._
import quasar.ejson
import quasar.ejson._
import quasar.Predef._
import quasar.fp._
import quasar.std.StdLib._

import matryoshka._
import scalaz._

sealed trait MapFunc[T[_[_]], A]

final case class Nullary[T[_[_]], A](ejson: T[EJson]) extends MapFunc[T, A]

sealed trait Unary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
}
sealed trait Binary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
  def a2: A
}
sealed trait Ternary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
  def a2: A
  def a3: A
}

object MapFunc {
  import MapFuncs._

  // TODO: The `T` passed to MapFunc and the `T` wrapping MapFunc should be distinct
  // def normalize[T[_[_]]: Recursive](implicit EJ: Equal[T[EJson]]):
  //     MapFunc[T, T[MapFunc[T, ?]]] => Option[MapFunc[T, T[MapFunc[T, ?]]]] = {
  //   val mf = new MapFuncs[T, T[MapFunc[T, ?]]]
  //   import mf._
  //   {
  //     case ConcatArrays(as) =>
  //       as.find {
  //         case Embed(ConcatArrays(_)) => true
  //         case _                      => false
  //       }.map(_ => ConcatArrays(as >>= {
  //         case Embed(ConcatArrays(moreAs)) => moreAs
  //         case a                           => List(a)
  //       }))
  //     case ConcatObjects(as) =>
  //       as.find {
  //         case Embed(ConcatObjects(_)) => true
  //         case _                       => false
  //       }.map(_ => ConcatObjects(as >>= {
  //         case Embed(ConcatObjects(moreAs)) => moreAs
  //         case a                            => List(a)
  //       }))
  //     case ProjectField(Embed(ConcatObjects(objs)), Embed(Nullary(field))) =>
  //       objs >>= {
  //         case Embed(MakeObject(Embed(Nullary(src)), value)) =>
  //           if (field ≟ src) value else Nil
  //         case x => List(x)
  //       }
  //   }
  // }

  implicit def functor[T[_[_]]]: Functor[MapFunc[T, ?]] = new Functor[MapFunc[T, ?]] {
    def map[A, B](fa: MapFunc[T, A])(f: A => B): MapFunc[T, B] =
      fa match {
        case Nullary(v) => Nullary[T, B](v)
        case Negate(a1) => Negate(f(a1))
        case MakeObject(a1, a2) => MakeObject(f(a1), f(a2))
        case ConcatObjects(a1, a2) => ConcatObjects(f(a1), f(a2))
        case ProjectField(a1, a2) => ProjectField(f(a1), f(a2))
        case Eq(a1, a2) => Eq(f(a1), f(a2))
        case x => { scala.Predef.print(s">>>>>>>>>>> got a functor $x"); ??? }
      }
  }

  implicit def equal[T[_[_]], A](implicit eqTEj: Equal[T[EJson]]):
      Delay[Equal, MapFunc[T, ?]] =
    new Delay[Equal, MapFunc[T, ?]] {
      // TODO this is wrong - we need to define equality on a function by function basis
      def apply[A](in: Equal[A]): Equal[MapFunc[T, A]] = Equal.equal {
        case (Nullary(v1), Nullary(v2)) => v1.equals(v2)
        case (Negate(a1), Negate(a2)) => in.equal(a1, a1)
        case (_, _) => false
      }
    }

  implicit def show[T[_[_]]](implicit shEj: Show[T[EJson]]): Delay[Show, MapFunc[T, ?]] =
    new Delay[Show, MapFunc[T, ?]] {
      def apply[A](sh: Show[A]): Show[MapFunc[T, A]] = Show.show {
        case Nullary(v) => Cord("Nullary(") ++ shEj.show(v) ++ Cord(")")
        case Negate(a1) => Cord("Negate(") ++ sh.show(a1) ++ Cord(")")
        case MakeObject(a1, a2) => Cord("MakeObject(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ConcatObjects(a1, a2) => Cord("ConcatObjects(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ProjectField(a1, a2) => Cord("ProjectField(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Eq(a1, a2) => Cord("Eq(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case x => { scala.Predef.print(s">>>>>>>>>>> got a show $x"); ??? }
      }
    }

  def translateUnaryMapping[T[_[_]], A]: UnaryFunc => A => MapFunc[T, A] = {
    {
      case date.Date => Date(_)
      case date.Time => Time(_)
      case date.Timestamp => Timestamp(_)
      case date.Interval => Interval(_)
      case date.TimeOfDay => TimeOfDay(_)
      case date.ToTimestamp => ToTimestamp(_)
      case math.Negate => Negate(_)
      case relations.Not => Not(_)
      case string.Length => Length(_)
      case string.Lower => Lower(_)
      case string.Upper => Upper(_)
      case string.Boolean => Bool(_)
      case string.Integer => Integer(_)
      case string.Decimal => Decimal(_)
      case string.Null => Null(_)
      case string.ToString => ToString(_)
      case structural.MakeArray => MakeArray(_)
    }
  }

  def translateBinaryMapping[T[_[_]], A]: BinaryFunc => (A, A) => MapFunc[T, A] = {
    {
      // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
      //     about the second. And it shouldn’t even have two in LP.
      case array.ArrayLength => (a, b) => Length(a)
      case date.Extract => Extract(_, _)
      case math.Add => Add(_, _)
      case math.Multiply => Multiply(_, _)
      case math.Subtract => Subtract(_, _)
      case math.Divide => Divide(_, _)
      case math.Modulo => Modulo(_, _)
      case math.Power => Power(_, _)
      case relations.Eq => Eq(_, _)
      case relations.Neq => Neq(_, _)
      case relations.Lt => Lt(_, _)
      case relations.Lte => Lte(_, _)
      case relations.Gt => Gt(_, _)
      case relations.Gte => Gte(_, _)
      case relations.IfUndefined => IfUndefined(_, _)
      case relations.And => And(_, _)
      case relations.Or => Or(_, _)
      case relations.Coalesce => Coalesce(_, _)
      case set.In => In(_, _)
      case set.Within => Within(_, _)
      case set.Constantly => Constantly(_, _)
      case structural.MakeObject => MakeObject(_, _)
      case structural.ObjectConcat => ConcatObjects(_, _)
      case structural.ArrayProject => ProjectIndex(_, _)
      case structural.ObjectProject => ProjectField(_, _)
      case structural.DeleteField => DeleteField(_, _)
      case string.Concat
         | structural.ArrayConcat
         | structural.ConcatOp => ConcatArrays(_, _)
    }
  }

  def translateTernaryMapping[T[_[_]], A]:
      TernaryFunc => (A, A, A) => MapFunc[T, A] = {
    {
      case relations.Between => Between(_, _, _)
      case relations.Cond    => Cond(_, _, _)
      case string.Like       => Like(_, _, _)
      case string.Search     => Search(_, _, _)
      case string.Substring  => Substring(_, _, _)
    }
  }
}

// TODO we should statically verify that these have a `DimensionalEffect` of `Mapping`
object MapFuncs {
  // array
  final case class Length[T[_[_]], A](a1: A) extends Unary[T, A]

  // date
  final case class Date[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Time[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Timestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Interval[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class TimeOfDay[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class ToTimestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Extract[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // math
  final case class Negate[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Add[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Multiply[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Subtract[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Divide[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Modulo[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Power[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // relations
  final case class Not[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Eq[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Neq[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Lt[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Lte[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Gt[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Gte[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class IfUndefined[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class And[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Or[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Coalesce[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Between[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]
  final case class Cond[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]

  // set
  final case class In[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Within[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class Constantly[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // string
  final case class Lower[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Upper[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Bool[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Integer[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Decimal[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Null[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class ToString[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Like[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]
  final case class Search[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]
  final case class Substring[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]

  // structural
  final case class MakeArray[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class MakeObject[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class ConcatArrays[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class ConcatObjects[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class ProjectIndex[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class ProjectField[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  final case class DeleteField[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // helpers & QScript-specific
  final case class DupMapKeys[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class DupArrayIndices[T[_[_]], A](a1: A) extends Unary[T, A]
  final case class Range[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  final case class Guard[T[_[_]], A](a1: A, pattern: Type, a2: A, a3: A)
      extends Ternary[T, A]

  def StrLit[T[_[_]], A](str: String)(implicit T: Corecursive[T]) =
    Nullary[T, A](CommonEJson.inj(ejson.Str[T[EJson]](str)).embed)
}

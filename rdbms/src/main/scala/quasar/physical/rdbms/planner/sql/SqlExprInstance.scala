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

package quasar.physical.rdbms.planner.sql

import scalaz._, Scalaz._

trait SqlExprInstances extends SqlExprTraverse with SqlExprRenderTree

trait SqlExprTraverse {
  import SqlExpr._, Select._, Case._

  implicit val traverse: Traverse[SqlExpr] = new Traverse[SqlExpr] {
    def traverseImpl[G[_], A, B](
        fa: SqlExpr[A]
    )(
        f: A => G[B]
    )(
        implicit G: Applicative[G]
    ): G[SqlExpr[B]] = fa match {
      case Null()              => G.point(Null())
      case Obj(m)              => m.traverse(_.bitraverse(f, f)) ∘ (l => Obj(l))
      case Constant(d)         => G.point(Constant(d))
      case Id(str)             => G.point(Id(str))
      case RegexMatches(a1, a2) => (f(a1) ⊛ f(a2))(RegexMatches(_, _))
      case ConcatStr(a1, a2)   => (f(a1) ⊛ f(a2))(ConcatStr(_, _))
      case Time(a1)            => f(a1) ∘ Time.apply
      case Refs(srcs)          =>  srcs.traverse(f) ∘ Refs.apply
      case Table(name)         => G.point(Table(name))
      case IsNotNull(v)        => f(v) ∘ IsNotNull.apply
      case IfNull(v)           => v.traverse(f) ∘ (IfNull(_))
      case RowIds()            => G.point(RowIds())
      case AllCols(v)          => G.point(AllCols(v))
      case NumericOp(op, left, right) => (f(left) ⊛ f(right))(NumericOp(op, _, _))
      case Mod(a1, a2)         => (f(a1) ⊛ f(a2))(Mod.apply)
      case Pow(a1, a2)         => (f(a1) ⊛ f(a2))(Pow.apply)
      case Neg(v)              => f(v) ∘ Neg.apply
      case WithIds(v)          => f(v) ∘ WithIds.apply

      case Select(selection, from, filterOpt) =>
        val sel = f(selection.v) ∘ (i => Selection(i, selection.alias ∘ (a => Id[B](a.v))))
        (sel ⊛
          (f(from.v) ∘ (From(_, from.alias ∘ (a => Id[B](a.v))))) ⊛
          filterOpt.traverse(i => f(i.v) ∘ Filter.apply))(
          Select(_, _, _)
        )

      case SelectRow(selection, from) =>
        val sel = f(selection.v) ∘ (i => Selection(i, selection.alias ∘ (a => Id[B](a.v))))
        (sel ⊛
          (f(from.v) ∘ (From(_, from.alias ∘ (a => Id[B](a.v))))))(
          SelectRow(_, _)
        )

      case Case(wt, Else(e)) =>
        (wt.traverse { case WhenThen(w, t) => (f(w) ⊛ f(t))(WhenThen(_, _)) } ⊛
          f(e)
          )((wt, e) =>
          Case(wt, Else(e))
        )
    }
  }
}

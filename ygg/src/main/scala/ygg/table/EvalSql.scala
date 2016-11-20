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

package ygg.table

import ygg._, common._, trans._
import quasar._, sql._

final case class EvalSql[T: TableRep](table: T) {
  val C = companionOf[T]

  def eval(sql: Fix[Sql]): T = {
    println(sql.render.draw mkString "\n")
    table transform (sql cata query)
  }

  object ProjField {
    def unapply(x: TransSpec1): Option[CPathField] = x match {
      case WrapObject(DerefObjectStatic(Leaf(Source), CPathField(n1)), n2) if n1 == n2 => Some(CPathField(n1))
      case _                                                                           => None
    }
  }
  object GroupedBy {
    def unapply(gb: sql.GroupBy[TransSpec1]): Option[Seq[TransSpec1]] = {
      Some(gb.keys map { case ProjField(k) => ID \ k })
    }
  }

  def fieldProj(p: Proj[TransSpec1]): TransSpec1 = p match {
    case Proj(ProjField(f), Some(alias)) => ID \ f as alias
    case Proj(ProjField(f), None)        => ID \ f as f.name
  }
  def valueProj(p: Proj[TransSpec1]): TransSpec1 = p match {
    case Proj(_, Some(_))         => fieldProj(p)
    case Proj(ProjField(f), None) => ID \ f
    case Proj(spec, None)         => spec
  }
  def mergeProj(ps: Seq[Proj[TransSpec1]]): TransSpec1 = ps match {
    case Seq()  => ID
    case Seq(x) => valueProj(x)
    case ps     => OuterObjectConcat(ps map fieldProj: _*)
  }
  def mergeSpec[A](ps: Seq[TransSpec1]): TransSpec1 = ps match {
    case Seq()  => ID
    case Seq(x) => x
    case ps     => OuterObjectConcat(ps: _*)
  }
  def query(x: Sql[TransSpec1]): TransSpec1 = x match {
    case Ident(name)                                                          => ID \ CPathField(name) as name
    case IntLiteral(v)                                                        => ConstLiteral(v, ID)
    case FloatLiteral(v)                                                      => ConstLiteral(v, ID)
    case StringLiteral(v)                                                     => ConstLiteral(v, ID)
    case NullLiteral()                                                        => ConstLiteral(null, ID)
    case BoolLiteral(v)                                                       => ConstLiteral(v, ID)
    case SetLiteral(exprs)                                                    => mergeSpec(exprs)
    case Splice(None)                                                         => ID
    case Splice(Some(expr))                                                   => expr
    case Select(SelectAll, projs, _, None, None, None)                        => mergeProj(projs)
    case Select(SelectAll, projs, _, None, Some(GroupedBy(keys)), None)       => mergeSpec(keys ++ (projs map (_.expr)))
    case Select(isDistinct, projections, relations, filter, groupBy, orderBy) => ???
    case ArrayLiteral(exprs)                                                  => ???
    case MapLiteral(exprs)                                                    => ???
    case InvokeFunction(name, args)                                           => ???
    case Match(expr, cases, optDefault)                                       => ???
    case Switch(cases, optDefault)                                            => ???
    case Binop(lhs, rhs, op)                                                  => ???
    case Unop(expr, op)                                                       => ???
    case Vari(name)                                                           => ???
    case Let(name, form, body)                                                => ???
  }
}

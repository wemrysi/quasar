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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.expression.DocVar

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

package object accumulator {

  def rewriteGroupRefs[EX[_]: Functor](t: AccumOp[Fix[EX]])(applyVar: PartialFunction[DocVar, DocVar])
      (implicit exprOps: ExprOpOps.Uni[EX]): AccumOp[Fix[EX]] =
    t.map(_.cata(exprOps.rewriteRefs(applyVar)))

  private def wrapInLet(bson: Bson): Bson =
    Bson.Doc("$let" -> Bson.Doc(
      "vars" -> Bson.Doc("a" -> bson),
      "in" -> Bson.Text("$$a")))

  // We can't use arrays directly inside accumulators because of
  // https://jira.mongodb.org/browse/SERVER-23839
  // Ideally this wrapping would happen before we have translated to Bson
  // but the trade-off is that in that case this wrapping needs to occur at all
  // call-sites which would be prone to errors.
  private def wrapArrInLet(bson: Bson): Bson = bson match {
    case a:Bson.Arr => wrapInLet(a)
    case x => x
  }

  val groupBsonƒ: AccumOp[Bson] => Bson = {
    case $addToSet(value) => Bson.Doc("$addToSet" -> wrapArrInLet(value))
    case $push(value)     => Bson.Doc("$push" -> wrapArrInLet(value))
    case $first(value)    => Bson.Doc("$first" -> wrapArrInLet(value))
    case $last(value)     => Bson.Doc("$last" -> wrapArrInLet(value))
    case $max(value)      => Bson.Doc("$max" -> wrapArrInLet(value))
    case $min(value)      => Bson.Doc("$min" -> wrapArrInLet(value))
    case $avg(value)      => Bson.Doc("$avg" -> wrapArrInLet(value))
    case $sum(value)      => Bson.Doc("$sum" -> wrapArrInLet(value))
  }

  def groupBson[EX[_]: Functor](g: AccumOp[Fix[EX]])(implicit exprOps: ExprOpOps.Uni[EX]) =
    groupBsonƒ(g.map(_.cata(exprOps.bson)))
}

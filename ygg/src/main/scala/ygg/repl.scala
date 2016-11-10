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

package ygg

import common._, json._, table._, trans._
import scalaz._
import TableData.fromJValues
import quasar.sql._
import matryoshka.Fix
import ygg.macros.JValueMacros

object repl {
  lazy val zips100   = TableData.fromFile(new jFile("it/src/main/resources/tests/smallZips.data"))
  lazy val zips500   = TableData.fromFile(new jFile("it/src/main/resources/tests/zips500.data"))
  lazy val zips3669  = TableData.fromFile(new jFile("it/src/main/resources/tests/largeZips.data"))
  lazy val zips29353 = TableData.fromFile(new jFile("it/src/main/resources/tests/zips.data"))
  lazy val patients  = TableData.fromJValues(
    JParser.parseUnsafe(new jFile("macros/src/test/resources/patients-mini.json") slurpString).asArray.elements
  )

  // case Select[A](
  //   isDistinct:  IsDistinct,
  //   projections: List[Proj[A]],
  //   relations:   Option[SqlRelation[A]],
  //   filter:      Option[A],
  //   groupBy:     Option[GroupBy[A]],
  //   orderBy:     Option[OrderBy[A]]
  // )
  // case Vari[A](symbol: String)                                     =>
  // case SetLiteral[A](exprs: List[A])                               =>
  // case ArrayLiteral[A](exprs: List[A])                             =>
  // case MapLiteral[A](exprs: List[(A, A)])                          =>
  // case Splice[A](expr: Option[A])                                  =>
  // case Binop[A](lhs: A, rhs: A, op: BinaryOperator)                =>
  // case Unop[A](expr: A, op: UnaryOperator)                         =>
  // case Ident[A](name: String)                                      =>
  // case InvokeFunction[A](name: String, args: List[A])              =>
  // case Match[A](expr: A, cases: List[Case[A]], default: Option[A]) =>
  // case Switch[A](cases: List[Case[A]], default: Option[A])         =>
  // case Let[A](name: String, form: A, body: A)                      =>
  // case IntLiteral[A](v: Long)                                      =>
  // case FloatLiteral[A](v: Double)                                  =>
  // case StringLiteral[A](v: String)                                 =>
  // case NullLiteral[A]()                                            =>
  // case BoolLiteral[A](value: Boolean)                              =>

  def sqlQuery(query: String): Fix[Sql] = fixParser.parse(Query(query)).toOption.get
  def query1 = sqlQuery("SELECT owner.name, car.name from owners as owner join cars as car on car._id = owner.carId")

  def moduloN(n: Long) = cf.math.Mod applyr CLong(n)
  def equalsN(n: Long) = cf.std.Eq applyr CLong(n)
  def divisibleBy(n: Long) = moduloN(n) andThen equalsN(0)

  implicit final class JvalueInterpolator(sc: StringContext) {
    def json(args: Any*): JValue             = macro JValueMacros.jsonInterpolatorImpl
    def jsonMany(args: Any*): Vector[JValue] = macro JValueMacros.jsonManyInterpolatorImpl
  }

  def medals      = fromJValues(medalsIn)
  def medalsMerge = MergeTable(grouping)(evaluator)

  def medalsIn = jsonMany"""
    {"key":[5908438637678314371],"value":{"Edition":"2000","Gender":"Men"}}
    {"key":[5908438637678314372],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314373],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314374],"value":{"Edition":"2004","Gender":"Women"}}
    {"key":[5908438637678314375],"value":{"Edition":"2000","Gender":"Women"}}
    {"key":[5908438637678314376],"value":{"Edition":"1996","Gender":"Women"}}
    {"key":[5908438637678314377],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314378],"value":{"Edition":"2004","Gender":"Men"}}
    {"key":[5908438637678314379],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314380],"value":{"Edition":"2008","Gender":"Women"}}
  """.toStream

  def medalsOut = jsonMany"""
    {"key":[],"value":{"year":"1996","ratio":139.0}}
    {"key":[],"value":{"year":"2000","ratio":126.0}}
    {"key":[],"value":{"year":"2004","ratio":122.0}}
    {"key":[],"value":{"year":"2008","ratio":119.0}}
  """


  def grouping = {
    def genderFilter(str: String) = Filter(EqualLiteral(ID \ 'value \ 'Gender, CString(str), false))
    def targetTrans = InnerObjectConcat(
      root delete "value",
      ID \ 'value \ 'Gender as "value"
    )
    def mkSource(groupId: Int, key: String, value: String) = GroupingSource(
      medals,
      'key,
      Some(targetTrans),
      groupId = groupId,
      GroupKeySpecSource(key, genderFilter(value)) && GroupKeySpecSource("1" -> ID \ 'value \ 'Edition)
    )
    GroupingAlignment.intersect(
      mkSource(0, "extra0", "Men"),
      mkSource(2, "extra1", "Women")
    )
  }
  def evaluator(key: RValue, partition: GroupId => Need[TableData]) = {
    val K0 = RValue.fromJValue(json"""{"1":"1996","extra0":true,"extra1":true}""")
    val K1 = RValue.fromJValue(json"""{"1":"2000","extra0":true,"extra1":true}""")
    val K2 = RValue.fromJValue(json"""{"1":"2004","extra0":true,"extra1":true}""")
    val K3 = RValue.fromJValue(json"""{"1":"2008","extra0":true,"extra1":true}""")

    val r0 = fromJValues(jsonMany"""{"key":[],"value":{"year":"1996","ratio":139.0}}""")
    val r1 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2000","ratio":126.0}}""")
    val r2 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2004","ratio":122.0}}""")
    val r3 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2008","ratio":119.0}}""")

    Need {
      key match {
        case K0 => r0
        case K1 => r1
        case K2 => r2
        case K3 => r3
        case _  => abort("Unexpected group key")
      }
    }
  }

  implicit def liftCNum(n: Int): CNum = CNum(BigDecimal(n))

  implicit class TableSelectionOps[T: TableRep](val table: T) {
    def p(): Unit                = table.dump()
    def apply(f: TransSpec1): T  = map(f)
    def map(f: TransSpec1): T    = table transform f

    def take(n: Long): T = if (n <= 0L) table.companion.empty else table.takeRange(0L, n)
    def drop(n: Long): T = if (n <= 0L) table else table.takeRange(n, Long.MaxValue)

    def filterConst(spec: TransSpec1, value: CValue) = map(
      Cond(
        trans.Equal(spec, ConstLiteral(value, ID)),
        ID,
        ID \ 'ConstantFalse // XXX FIXME
      )
    )
  }
}

//     def deepMap(pf: MaybeSelf[F1]): T = map(root deepMap pf)
//     def deepMap1(fn: CF1): T          = map(root deepMap1 fn)
//     def delete(p: JPathField): T      = map(root delete CPathField(p.name))
//     def filter(p: F1): T              = map(root filter p)
//     def isEqual(that: CValue)         = map(root isEqual that)
//     def isType(tp: JType)             = map(root isType tp)
//     def map(f: TransSpec1): T         = table transform f
//     def map1(fn: CF1)                 = map(root map1 fn)
//     def select(field: CPathField): T  = map(root select field)
//     def select(name: String): T       = select(CPathField(name))

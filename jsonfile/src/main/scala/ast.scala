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

 package quasar.physical.jsonfile.fs

 import ygg._, common._, table._, json._
 import quasar._
 import scalaz._

sealed abstract class YggQ

object YggQ {
  implicit def liftTable(x: TableData): YggQ = TableQ(x)

  final case class JValues(jvs: Seq[JValue]) extends YggQ
  final case class TableQ(td: TableData) extends YggQ
  final case class Spec(spec: TransSpec1) extends YggQ
  final case class Var(sym: Sym) extends YggQ
  final case class Step(override val toString: String) extends YggQ
  final case class Expression(override val toString: String) extends YggQ
  final case class StringLit(str: String) extends YggQ {
    override def toString = s""""$str""""
  }

  class Eval(files: Map[FPath, YggQ], args: Sym => YggQ) extends LpEval[YggQ] with EvalMapFunc[Fix, YggQ] {
    private type A = YggQ

    import TableData._

    val corecursive    = implicitly[Corecursive[Fix]]
    val recursive      = implicitly[Recursive[Fix]]
    val booleanAlgebra = BooleanAlgebra.undefined[A](???)
    val numericAlgebra = NumericAlgebra.undefined[A](???)
    val timeAlgebra    = TimeAlgebra.undefined[A](???, ???)
    val order          = (null: Ord[A])
    val nullA          = TableQ(constNull)

    val StrPrism    = Prism.partial[A, String] { case StringLit(s) => s } (StringLit)
    val RegexPrism  = Prism[A, Regex](_ => None)(_ => ???)
    val BoolPrism   = Prism[A, Boolean](booleanAlgebra.toBool)(constBoolean(_))
    val DecPrism    = Prism[A, BigDecimal](_ => None)(x => constDouble(x.doubleValue))
    val Int32Prism  = Prism[A, Int](_ => None)(x => constLong(x.toLong))
    val Int64Prism  = Prism[A, Long](_ => None)(constLong(_))
    val BigIntPrism = Prism[A, BigInt](_ => None)(x => constLong(x.longValue))
    val SymPrism    = Prism.partial[A, Sym] { case Var(sym) => sym }(Var)

    def free(sym: Sym): A                                  = args(sym)
    def undef: A                                           = ???
    def bind(let: Sym, form: A, in: A): A                  = ???
    def typecheck(expr: A, typ: Type, ifp: A, elsep: A): A = ???
    def read(path: FPath): A                               = files(path)
    def constant(data: Data): A                            = JValues(Seq(dataToJValue(data)))
  }

  implicit val show: Show[YggQ] = Show.showFromToString
}

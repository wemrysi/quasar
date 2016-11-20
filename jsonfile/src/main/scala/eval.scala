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

import ygg._, table._, common._
import quasar._
import quasar.frontend.{ logicalplan => lp }
import quasar.qscript.MapFunc

abstract class LpEval[A] extends quasar.qscript.TTypes[Fix] {
  def undef: A
  def read(path: FPath): A
  def constant(data: Data): A
  def bind(let: Sym, form: A, in: A): A
  def typecheck(expr: A, typ: Type, ifp: A, elsep: A): A
  def free(sym: Sym): A

  def stepMF: Algebra[MapFunc, A]
  def stepLP: Algebra[LP, A] = {
    case lp.Read(path)                           => read(path)
    case lp.Constant(data)                       => constant(data)
    case lp.Free(sym)                            => free(sym)
    case lp.Let(let, form, in)                   => bind(let, form, in)
    case lp.Typecheck(expr, typ, cont, fallback) => typecheck(expr, typ, cont, fallback)
    case UnaryMF(f, a1)                          => stepMF(MapFunc.translateUnaryMapping(f)(a1))
    case BinaryMF(f, a1, a2)                     => stepMF(MapFunc.translateBinaryMapping(f)(a1, a2))
    case TernaryMF(f, a1, a2, a3)                => stepMF(MapFunc.translateTernaryMapping(f)(a1, a2, a3))
  }

  def eval(lp: Fix[LP]): A = {
    println(lp.render.draw mkString "\n")
    lp cata stepLP
  }
}

object LpEval extends quasar.qscript.TTypes[Fix] {
  class Impl[A : TableRep : Ord](files: Map[FPath, A], args: Sym => A) extends LpEval[A] with EvalMapFunc[Fix, A] {
    val C = companionOf[A]
    import C._

    val corecursive    = implicitly[Corecursive[Fix]]
    val recursive      = implicitly[Recursive[Fix]]
    val booleanAlgebra = BooleanAlgebra.undefined[A](???)
    val numericAlgebra = NumericAlgebra.undefined[A](???)
    val timeAlgebra    = TimeAlgebra.undefined[A](???, ???)
    val order          = implicitly[Ord[A]]
    val nullA          = constNull

    val StrPrism    = Prism[A, String](_ => None)(constString(_))
    val RegexPrism  = Prism[A, Regex](_ => None)(_ => ???)
    val BoolPrism   = Prism[A, Boolean](booleanAlgebra.toBool)(constBoolean(_))
    val DecPrism    = Prism[A, BigDecimal](_ => None)(x => constDouble(x.doubleValue))
    val Int32Prism  = Prism[A, Int](_ => None)(x => constLong(x.toLong))
    val Int64Prism  = Prism[A, Long](_ => None)(constLong(_))
    val BigIntPrism = Prism[A, BigInt](_ => None)(x => constLong(x.longValue))
    val SymPrism    = Prism[A, Sym](_ => None)(_ => ???)

    def readDirect(f: FPath): A = fromFile(f.toJavaFile)

    def free(sym: Sym): A                                  = args(sym)
    def undef: A                                           = ???
    def bind(let: Sym, form: A, in: A): A                  = ???
    def typecheck(expr: A, typ: Type, ifp: A, elsep: A): A = ???
    def read(path: FPath): A                               = files(path)
    def constant(data: Data): A = data match {
      case Data.Int(x)  => constLong(x.longValue)
      case Data.Dec(x)  => constDouble(x.doubleValue)
      case Data.Str(x)  => constString(x)
      case Data.Bool(x) => constBoolean(x)
      case Data.Null    => constNull
    }

  }
}

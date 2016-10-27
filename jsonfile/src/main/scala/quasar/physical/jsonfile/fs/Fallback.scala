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

import quasar.Predef._
import quasar.Type
import quasar.fs._
import quasar.qscript.{ MapFuncs => mf }
import quasar.ejson.EJson
import matryoshka._
import scalaz._, Scalaz._

trait Extractor[A, B] {
  def unapply(x: A): Option[B]
}
object Extractor {
  def partial[A, B](pf: PartialFunction[A, B]): Extractor[A, B] = apply(pf.lift)
  def apply[A, B](f: A => Option[B]): Extractor[A, B]           = new Extractor[A, B] { def unapply(x: A) = f(x) }
}

abstract class PExtractor[A, B](pf: PartialFunction[A, B]) extends Extractor[A, B] {
  def unapply(x: A): Option[B] = pf lift x
}

trait Fresh[T[_[_]], F[_], Rep] extends quasar.qscript.TTypes[T] {
  def TODO: Nothing = scala.Predef.???

  implicit def monad: Monad[F]
  implicit def booleanAlgebra: BooleanAlgebra[Rep]
  implicit def numericAlgebra: NumericAlgebra[Rep]
  implicit def orderAlgebra: OrderAlgebra[Rep]

  // implicit def booleanAlgebraF: BooleanAlgebra[F[Rep]] = BooleanAlgebra.lift[F, Rep](booleanAlgebra)
  // implicit def numericAlgebraF: NumericAlgebra[F[Rep]] = NumericAlgebra.lift[F, Rep](numericAlgebra)
  // implicit def orderAlgebraF: OrderAlgebra[F[Rep]]     = OrderAlgebra.lift[F, Rep](orderAlgebra)

  type QsAlgebra[QS[_]]   = AlgebraM[F, QS, Rep]
  type QsExtractor[QS[_]] = Extractor[QS[Rep], F[Rep]]

  def queryFile: QueryFile ~> F
  def readFile: ReadFile ~> F
  def writeFile: WriteFile ~> F
  def manageFile: ManageFile ~> F
  def fileSystem: FileSystem ~> F

  def undefined: Rep
  def isUndefined(value: Rep): Boolean
  def hasType(scrutinee: Rep, tpe: Type): Boolean
  def constant(literal: T[EJson]): Rep
  def asBoolean(value: Rep): F[Boolean]

  val MF: MapFuncExtractors

  trait MapFuncExtractors {
    type Ex = QsExtractor[MapFunc]

    val Time: Ex
    val Math: Ex
    val Bool: Ex
    val Str: Ex
    val Structural: Ex
    val Special: Ex
  }

  def mapFunc: QsAlgebra[MapFunc] = {
    case MF.Time(x)       => x
    case MF.Math(x)       => x
    case MF.Bool(x)       => x
    case MF.Str(x)        => x
    case MF.Structural(x) => x
    case MF.Special(x)    => x
  }
}

trait FreshImpls[T[_[_]], F[_], Rep] extends Fresh[T, F, Rep] {
  self =>

  private implicit def liftRep(x: Rep): F[Rep] = x.point[F]
  import BooleanAlgebra._, NumericAlgebra._, OrderAlgebra._

  object MF extends MapFuncExtractors {
    def mk(pf: PartialFunction[MapFunc[Rep], F[Rep]]) = Extractor partial pf

    val Time = mk {
      case mf.Date(s)                     => TODO
      case mf.Interval(s)                 => TODO
      case mf.Length(len)                 => TODO
      case mf.Now()                       => TODO
      case mf.Time(s)                     => TODO
      case mf.TimeOfDay(dt)               => TODO
      case mf.Timestamp(s)                => TODO
      case mf.ToTimestamp(millis)         => TODO
      case mf.ExtractCentury(time)        => TODO
      case mf.ExtractDayOfMonth(time)     => TODO
      case mf.ExtractDayOfWeek(time)      => TODO
      case mf.ExtractDayOfYear(time)      => TODO
      case mf.ExtractDecade(time)         => TODO
      case mf.ExtractEpoch(time)          => TODO
      case mf.ExtractHour(time)           => TODO
      case mf.ExtractIsoDayOfWeek(time)   => TODO
      case mf.ExtractIsoYear(year)        => TODO
      case mf.ExtractMicroseconds(time)   => TODO
      case mf.ExtractMillennium(time)     => TODO
      case mf.ExtractMilliseconds(time)   => TODO
      case mf.ExtractMinute(time)         => TODO
      case mf.ExtractMonth(time)          => TODO
      case mf.ExtractQuarter(time)        => TODO
      case mf.ExtractSecond(time)         => TODO
      case mf.ExtractTimezone(time)       => TODO
      case mf.ExtractTimezoneHour(time)   => TODO
      case mf.ExtractTimezoneMinute(time) => TODO
      case mf.ExtractWeek(time)           => TODO
      case mf.ExtractYear(time)           => TODO
    }
    val Math = mk {
      case mf.Negate(x)      => -x
      case mf.Add(x, y)      => x + y
      case mf.Multiply(x, y) => x * y
      case mf.Subtract(x, y) => x - y
      case mf.Divide(x, y)   => x / y
      case mf.Modulo(x, y)   => x % y
      case mf.Power(b, e)    => b ** e
    }
    val Bool = mk {
      case mf.Not(x)             => !x
      case mf.Eq(x, y)           => x === y
      case mf.Neq(x, y)          => x =/= y
      case mf.Lt(x, y)           => x < y
      case mf.Lte(x, y)          => x <= y
      case mf.Gt(x, y)           => x > y
      case mf.Gte(x, y)          => x >= y
      case mf.And(x, y)          => x && y
      case mf.Or(x, y)           => x || y
      case mf.Between(x, lo, hi) => lo <= x && x <= hi
      case mf.Within(item, arr)  => TODO
    }
    val Str = mk {
      case mf.Lower(s)                        => TODO
      case mf.Upper(s)                        => TODO
      case mf.Bool(s)                         => TODO
      case mf.Integer(s)                      => TODO
      case mf.Decimal(s)                      => TODO
      case mf.Null(s)                         => TODO
      case mf.ToString(value)                 => TODO
      case mf.Search(s, pattern, insensitive) => TODO
      case mf.Substring(s, offset, length)    => TODO
    }
    val Structural = mk {
      case mf.ConcatArrays(xs, ys)     => TODO
      case mf.ConcatMaps(xs, ys)       => TODO
      case mf.DeleteField(src, field)  => TODO
      case mf.DupArrayIndices(arr)     => TODO
      case mf.DupMapKeys(map)          => TODO
      case mf.MakeArray(arr)           => TODO
      case mf.MakeMap(key, value)      => TODO
      case mf.ProjectField(src, field) => TODO
      case mf.ProjectIndex(arr, idx)   => TODO
      case mf.Range(from, to)          => TODO
      case mf.ZipArrayIndices(arr)     => TODO
      case mf.ZipMapKeys(map)          => TODO
    }
    val Special = mk {
      case mf.Cond(p, ifp, elsep)               => Monad[F].ifM(asBoolean(p), ifp, elsep)
      case mf.Constant(lit)                     => constant(lit)
      case mf.Guard(scrutinee, tpe, ifp, elsep) => if (hasType(scrutinee, tpe)) ifp else elsep
      case mf.IfUndefined(value, alt)           => if (isUndefined(value)) alt else value
      case mf.Undefined()                       => undefined
    }
  }
}

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
import quasar.fs._
import quasar.qscript.{ MapFuncs => mf }
import matryoshka._
import matryoshka.Recursive.ops._
// import matryoshka.FunctorT.ops._
// import matryoshka.Corecursive.ops._
import scalaz._
import Scalaz._
import jawn.Facade
import java.time._

trait Fresh[T[_[_]], F[_], Rep] extends quasar.qscript.TTypes[T] {
  implicit def corecursive: Corecursive[T]
  implicit def recursive: Recursive[T]
  implicit def monad: Monad[F]
  implicit def booleanAlgebra: BooleanAlgebra[Rep]
  implicit def numericAlgebra: NumericAlgebra[Rep]
  implicit def timeAlgebra: TimeAlgebra[Rep]
  implicit def order: Order[Rep]
  implicit def facade: Facade[Rep]

  val types: FallbackTypes[Rep]
  val MF: MapFuncExtractors

  import types.undef

  lazy val BoolRep   = Extractor(types.bool.getOption)
  lazy val LongRep   = Extractor(types.long.getOption)
  lazy val StringRep = Extractor(types.string.getOption)

  type EJRep = EJson[Rep]

  implicit def liftBoolean(value: Boolean): F[Rep] = types.bool.set(value)(undef).point[F]
  implicit def liftLong(value: Long): F[Rep]       = types.long.set(value)(undef).point[F]
  implicit def liftString(value: String): F[Rep]   = types.string.set(value)(undef).point[F]
  implicit def liftEJson(value: EJRep): F[Rep]     = fromEJson(value).point[F]

  def fileSystem: FileSystem ~> F
  def fromEJson: Algebra[EJson, Rep]
  def toEJson: Coalgebra[EJson, Rep]

  type QsAlgebra[QS[_]]   = AlgebraM[F, QS, Rep]
  type QsExtractor[QS[_]] = Extractor[QS[Rep], F[Rep]]


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

trait Fallback[T[_[_]], F[_], Rep] extends Fresh[T, F, Rep] {
  self =>

  private implicit def liftRep(x: Rep): F[Rep] = x.point[F]

  import types.undef

  object MF extends MapFuncExtractors {
    def mk(pf: PartialFunction[MapFunc[Rep], F[Rep]]) = Extractor partial pf

    val Time = mk {
      case mf.Date(s)                      => TODO
      case mf.Interval(s)                  => TODO
      case mf.Length(s)                    => TODO
      case mf.Now()                        => nowMillis
      case mf.Time(s)                      => TODO
      case mf.TimeOfDay(dt)                => TODO
      case mf.Timestamp(StringRep(s))      => instantMillis(Instant.parse(s))
      case mf.ToTimestamp(LongRep(millis)) => zonedUtcFromMillis(millis).toString
      case mf.ExtractCentury(time)         => time.extractCentury
      case mf.ExtractDayOfMonth(time)      => time.extractDayOfMonth
      case mf.ExtractDayOfWeek(time)       => time.extractDayOfWeek
      case mf.ExtractDayOfYear(time)       => time.extractDayOfYear
      case mf.ExtractDecade(time)          => time.extractDecade
      case mf.ExtractEpoch(time)           => time.extractEpoch
      case mf.ExtractHour(time)            => time.extractHour
      case mf.ExtractIsoDayOfWeek(time)    => time.extractIsoDayOfWeek
      case mf.ExtractIsoYear(time)         => time.extractIsoYear
      case mf.ExtractMicroseconds(time)    => time.extractMicroseconds
      case mf.ExtractMillennium(time)      => time.extractMillennium
      case mf.ExtractMilliseconds(time)    => time.extractMilliseconds
      case mf.ExtractMinute(time)          => time.extractMinute
      case mf.ExtractMonth(time)           => time.extractMonth
      case mf.ExtractQuarter(time)         => time.extractQuarter
      case mf.ExtractSecond(time)          => time.extractSecond
      case mf.ExtractTimezone(time)        => time.extractTimezone
      case mf.ExtractTimezoneHour(time)    => time.extractTimezoneHour
      case mf.ExtractTimezoneMinute(time)  => time.extractTimezoneMinute
      case mf.ExtractWeek(time)            => time.extractWeek
      case mf.ExtractYear(time)            => time.extractYear
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
      case mf.And(x, y)          => x && y
      case mf.Or(x, y)           => x || y
      case mf.Eq(x, y)           => x === y
      case mf.Neq(x, y)          => x =/= y
      case mf.Lt(x, y)           => x < y
      case mf.Lte(x, y)          => x <= y
      case mf.Gt(x, y)           => x > y
      case mf.Gte(x, y)          => x >= y
      case mf.Between(x, lo, hi) => lo <= x && x <= hi
      case mf.Within(item, arr)  => TODO
    }
    val Str = mk {
      case mf.Lower(StringRep(s))                                 => s.toLowerCase
      case mf.Upper(StringRep(s))                                 => s.toUpperCase
      case mf.Bool(StringRep("true"))                             => true
      case mf.Bool(StringRep("false"))                            => false
      case mf.Bool(_)                                             => undef
      case mf.Integer(StringRep(s))                               => TODO // BigInt(s)
      case mf.Decimal(StringRep(s))                               => TODO // BigDecimal(s)
      case mf.Null(StringRep(s))                                  => TODO
      case mf.ToString(value)                                     => value.toString
      case mf.Search(StringRep(s), pattern, insensitive)          => TODO
      case mf.Substring(StringRep(s), LongRep(off), LongRep(len)) => s.slice(off.toInt, off.toInt + len.toInt): String
    }
    val Structural = mk {
      case mf.ConcatArrays(xs, ys)     => ejfuns.concat(toEJson(xs), toEJson(ys))
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
      case mf.Cond(BoolRep(p), ifp, elsep)  => p.fold(ifp, elsep)
      case mf.Cond(_, _, _)                 => undef
      case mf.Constant(lit)                 => lit cata fromEJson
      case mf.Guard(value, tpe, ifp, elsep) => types.hasType(value, tpe).fold(ifp, elsep)
      case mf.IfUndefined(value, alt)       => (value === undef).fold(alt, value)
      case mf.Undefined()                   => undef
    }
  }

  object ejfuns {
    import quasar.ejson._

    def concat(xs: EJRep, ys: EJRep): EJRep = (xs.run, ys.run) match {
      case (\/-(Arr(xs)), \/-(Arr(ys))) => Coproduct right Arr(xs ++ ys)
      case _                            => ???
    }
  }
}

object Fallback {
  def create[T[_[_]], F[_], Rep](fs: FileSystem ~> F, alg: Algebra[EJson, Rep], coalg: Coalgebra[EJson, Rep])(implicit
    RT: Recursive[T],
    CT: Corecursive[T],
    MO: Monad[F],
    NA: NumericAlgebra[Rep],
    BA: BooleanAlgebra[Rep],
    TA: TimeAlgebra[Rep],
    OA: Order[Rep],
    JF: Facade[Rep],
    FT: FallbackTypes[Rep]
  ) = new Fallback[T, F, Rep] {

    val fileSystem = fs
    val fromEJson  = alg
    val toEJson    = coalg
    val types      = FT

    implicit val recursive      = RT
    implicit val corecursive    = CT
    implicit val monad          = MO
    implicit val numericAlgebra = NA
    implicit val booleanAlgebra = BA
    implicit val timeAlgebra    = TA
    implicit val order          = OA
    implicit val facade         = JF
  }

  def free[T[_[_]]: Recursive : Corecursive, R: NumericAlgebra : BooleanAlgebra : TimeAlgebra : FallbackTypes : Order : Facade](
    alg: Algebra[EJson, R],
    coalg: Coalgebra[EJson, R]
  ) =
    create[T, InMemory.F, R](InMemory.fileSystem, alg, coalg)
}

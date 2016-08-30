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

package ygg.tests

import Gen.{ alphaLowerChar, oneOf, frequency, delay }
import ygg._, common._, json._
import ygg.data.Bits

object TableTestSupport extends TableGenerators {}

object JsonTestSupport extends JsonGenerators with TableGenerators {
  def arb[A](implicit z: Arbitrary[A]): Arbitrary[A] = z

  implicit def arbJValue: Arbitrary[JValue]         = Arbitrary(genJValue)
  implicit def arbJObject: Arbitrary[JObject]       = Arbitrary(genJObject)
  implicit def arbJPath: Arbitrary[JPath]           = Arbitrary(genJPath)
  implicit def arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(genBigDecimal)
}

trait TableGenerators {
  import ygg.table._

  def genSlice(refs: Seq[ColumnRef], sz: Int): Gen[Slice] = {
    val zero    = Nil: Gen[List[ColumnRef -> Column]]
    val gs      = refs map (cr => genColumn(cr, sz) ^^ (cr -> _))
    val genData = gs.foldLeft(zero)((res, g) => res >> (r => g ^^ (_ :: r)))

    genData ^^ (data => Slice(sz, data.toMap))
  }

  def genColumn(col: ColumnRef, size: Int): Gen[Column] = {
    def bs = Bits.range(0, size)
    col.ctype match {
      case CString       => arrayOfN(size, genString) ^^ (ArrayStrColumn(bs, _))
      case CBoolean      => arrayOfN(size, genBool) ^^ (ArrayBoolColumn(bs, _))
      case CLong         => arrayOfN(size, genLong) ^^ (ArrayLongColumn(bs, _))
      case CDouble       => arrayOfN(size, genDouble) ^^ (ArrayDoubleColumn(bs, _))
      case CDate         => arrayOfN(size, genLong) ^^ (ns => ArrayDateColumn(bs, ns map dateTime.fromMillis))
      case CPeriod       => arrayOfN(size, genLong) ^^ (ns => ArrayPeriodColumn(bs, ns map period.fromMillis))
      case CNum          => arrayOfN(size, genDouble) ^^ (ns => ArrayNumColumn(bs, ns map (v => BigDecimal(v))))
      case CNull         => genBitSet(size) ^^ (s => new BitsetColumn(s) with NullColumn)
      case CEmptyObject  => genBitSet(size) ^^ (s => new BitsetColumn(s) with EmptyObjectColumn)
      case CEmptyArray   => genBitSet(size) ^^ (s => new BitsetColumn(s) with EmptyArrayColumn)
      case CUndefined    => UndefinedColumn.raw
      case CArrayType(_) => abort("undefined")
    }
  }
}

trait JsonGenerators {
  def badPath(jv: JValue, p: JPath): Boolean = (jv, p.nodes) match {
    case (JArray(_), JPathField(_) +: _)   => true
    case (JObject(_), JPathIndex(_) +: _)  => true
    case (_, Seq())                        => false
    case (_, JPathField(name) +: xs)       => badPath(jv \ name, JPath(xs))
    case (JArray(ns), JPathIndex(i) +: xs) => (i > ns.length) || (i < ns.length && badPath(ns(i), JPath(xs))) || badPath(jarray(), JPath(xs))
    case (_, JPathIndex(i) +: xs)          => (i != 0) || badPath(jarray(), JPath(xs))
  }

  def genJPathNode: Gen[JPathNode] = frequency(
    1 -> (choose(0, 10) ^^ JPathIndex),
    9 -> (genIdent ^^ JPathField)
  )
  def genJPath: Gen[JPath] = genJPathNode * choose(0, 10) ^^ (xs => JPath(xs: _*))

  /** The delay wrappers are needed because we generate
    *  JValues recursively.
    */
  def genJValue: Gen[JValue] = frequency(
    5 -> genSimple,
    1 -> delay(genJArray),
    1 -> delay(genJObject)
  )
  def genJValueSeq: Gen[Seq[JValue]] = genJValue * (0 upTo 10)

  def genIdent: Gen[String]    = alphaLowerChar * choose(3, 8) ^^ (_.mkString)
  def genSimple: Gen[JValue]   = oneOf[JValue](JNull, genJNum, genJBool, genJString)
  def gen0To5: Gen[Int]        = choose(0, 5)
  def genJNum: Gen[JNum]       = genBigDecimal ^^ (x => JNum(x))
  def genJBool: Gen[JBool]     = genBool ^^ (x => JBool(x))
  def genJString: Gen[JString] = genIdent ^^ (s => JString(s))
  def genJField: Gen[JField]   = (genIdent, genPosInt, genJValue) >> ((name, id, value) => JField(s"$name$id", value))
  def genJObject: Gen[JObject] = genJField * gen0To5 ^^ (xs => JObject(xs: _*))
  def genJArray: Gen[JArray]   = genJValue * gen0To5 ^^ (xs => JArray(xs.toVector))
}

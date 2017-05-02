/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package quasar.yggdrasil
package jdbm3

import blueeyes._
import com.precog.common._
import quasar.precog.TestSupport._, Gen._

trait CValueGenerators {
  def maxArrayDepth = 3

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List,Seq[CValue]](size, values.map(_.toSeq))

  private def genNonArrayCValueType: Gen[CValueType[_]] = Gen.oneOf[CValueType[_]](CString, CBoolean, CLong, CDouble, CNum, CDate)

  def genCValueType(maxDepth: Int = maxArrayDepth, depth: Int = 0): Gen[CValueType[_]] = {
    if (depth >= maxDepth) genNonArrayCValueType else {
      frequency(0 -> (genCValueType(maxDepth, depth + 1) map (CArrayType(_))), 6 -> genNonArrayCValueType)
    }
  }

  def genCType: Gen[CType] = frequency(7 -> genCValueType(), 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

  def genValueForCValueType[A](cType: CValueType[A]): Gen[CWrappedValue[A]] = cType match {
    case CString  => genString map (CString(_))
    case CBoolean => genBool map (CBoolean(_))
    case CLong    => genLong map (CLong(_))
    case CDouble  => genDouble map (CDouble(_))
    case CNum     => for {
      scale  <- genInt
      bigInt <- genBigInt
    } yield CNum(BigDecimal(new java.math.BigDecimal(bigInt.bigInteger, scale - 1), java.math.MathContext.UNLIMITED))
    case CDate                => genPosLong ^^ (n => CDate(dateTime fromMillis n))
    case CArrayType(elemType) =>
      vectorOf(genValueForCValueType(elemType) map (_.value)) map { xs =>
        CArray(xs.toArray(elemType.classTag), CArrayType(elemType))
      }
    case CPeriod => abort("undefined")
  }

  def genCValue(tpe: CType): Gen[CValue] = tpe match {
    case tpe: CValueType[_] => genValueForCValueType(tpe)
    case CNull              => Gen.const(CNull)
    case CEmptyObject       => Gen.const(CEmptyObject)
    case CEmptyArray        => Gen.const(CEmptyArray)
    case invalid            => sys.error("No values for type " + invalid)
  }
}



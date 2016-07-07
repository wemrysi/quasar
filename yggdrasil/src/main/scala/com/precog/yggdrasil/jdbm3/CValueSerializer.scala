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
package com.precog.yggdrasil
package jdbm3

import org.slf4s.Logger

import org.apache.jdbm._
import org.joda.time.DateTime

import java.io.{DataInput,DataOutput}

import com.precog.common._
import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

/* I really hate using this, but the custom serialization code in JDBM3 is choking in
 * very weird ways when using "CType" as the format type. It appears that it "registers"
 * the CType class in the serializer, but on read it blows up because the new serializer
 * hasn't registered the same class and I can't figure out how to properly register it.
 * Using Byte values to indicate CType is somewhat of a hack, but it works
 *
 * DCB - 2012-07-27
 */
private[jdbm3] object CTypeMappings {
  final val FSTRING      = 0.toByte
  final val FBOOLEAN     = 1.toByte
  final val FLONG        = 2.toByte
  final val FDOUBLE      = 3.toByte
  final val FNUM         = 4.toByte
  final val FDATE        = 5.toByte
  final val FNULL        = 6.toByte
  final val FEMPTYOBJECT = 7.toByte
  final val FEMPTYARRAY  = 8.toByte
  final val FARRAY       = 9.toByte
  final val FPERIOD      = 10.toByte
  final val FUNDEFINED   = -1.toByte

  def flagFor(tpe: CType): Byte = tpe match {
    case CString      => FSTRING
    case CBoolean     => FBOOLEAN
    case CLong        => FLONG
    case CDouble      => FDOUBLE
    case CNum         => FNUM
    case CDate        => FDATE
    case CPeriod      => FPERIOD
    case CNull        => FNULL
    case CEmptyObject => FEMPTYOBJECT
    case CEmptyArray  => FEMPTYARRAY
    case CArrayType(_)=> FARRAY
    case CUndefined   => sys.error("Undefined is not a valid format")
  }

  def fromFlag(b: Byte): CType = b match {
    case FSTRING       => CString
    case FBOOLEAN      => CBoolean
    case FLONG         => CLong
    case FDOUBLE       => CDouble
    case FNUM          => CNum
    case FDATE         => CDate
    case FPERIOD       => CPeriod
    case FNULL         => CNull
    case FEMPTYOBJECT  => CEmptyObject
    case FEMPTYARRAY   => CEmptyArray
    case FARRAY        => sys.error("todo")
    case invalid       => sys.error(invalid + " is not a valid format")
  }
}

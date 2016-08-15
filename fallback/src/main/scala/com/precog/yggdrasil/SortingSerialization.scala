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

import blueeyes._
import org.mapdb._

final case class SortingKeyComparator(rowFormat: RowFormat, ascending: Boolean) extends Serializer[Array[Byte]] {
  def deserialize(input: DataInput2, available: Int): Array[Byte] = warn(s"deserialize($input, $available)")(???)
  def serialize(out: DataOutput2, value: Array[Byte]): Unit       = warn(s"serialize($out, $value)")(???)

  def compare(a: Array[Byte], b: Array[Byte]): Int =
    rowFormat.compare(a, b) |> (n => if (ascending) n else -n)
}

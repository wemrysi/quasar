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
package com.precog.niflheim

import blueeyes._, json._
import com.precog.common._
import com.precog.common.security.Authorities
import com.precog.util.PrecogUnit

import scalaz._
import scalaz.effect.IO
import java.util.concurrent.atomic._

case class Insert(batch: Seq[NIHDB.Batch], responseRequested: Boolean)

case object GetSnapshot

case class Block(id: Long, segments: Seq[Segment], stable: Boolean)

case object GetStatus
case class Status(cooked: Int, pending: Int, rawSize: Int)

case object GetStructure
case class Structure(columns: Set[(CPath, CType)])

sealed trait InsertResult
case class Inserted(offset: Long, size: Int) extends InsertResult
case object Skipped extends InsertResult

case object Quiesce

object NIHDB {
  final val projectionIdGen = new AtomicInteger()

  case class Batch(offset: Long, values: Seq[JValue])
}

trait NIHDB {
  def authorities: Authorities

  def insert(batch: Seq[NIHDB.Batch]): IO[PrecogUnit]

  def insertVerified(batch: Seq[NIHDB.Batch]): Future[InsertResult]

  def getSnapshot(): Future[NIHDBSnapshot]

  def getBlockAfter(id: Option[Long], cols: Option[Set[ColumnRef]]): Future[Option[Block]]

  def getBlock(id: Option[Long], cols: Option[Set[CPath]]): Future[Option[Block]]

  def length: Future[Long]

  def projectionId: Int

  def status: Future[Status]

  def structure: Future[Set[ColumnRef]]

  /**
    * Returns the total number of defined objects for a given `CPath` *mask*.
    * Since this punches holes in our rows, it is not simply the length of the
    * block. Instead we count the number of rows that have at least one defined
    * value at each path (and their children).
    */
  def count(paths0: Option[Set[CPath]]): Future[Long]

  def quiesce: IO[PrecogUnit]
}

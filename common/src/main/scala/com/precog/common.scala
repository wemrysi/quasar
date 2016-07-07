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
package com.precog

package object common {
  type ProducerId = Int
  type SequenceId = Int

  // akka encapsulation
  type ActorRef         = akka.actor.ActorRef
  type ActorSystem      = akka.actor.ActorSystem
  type Duration         = akka.util.Duration
  type ExecutionContext = akka.dispatch.ExecutionContext
  type Future[+A]       = akka.dispatch.Future[A]
  type Promise[A]       = akka.dispatch.Promise[A]
  type Timeout          = akka.util.Timeout
  val ActorSystem       = akka.actor.ActorSystem
  val AkkaProps         = akka.actor.Props
  val Await             = akka.dispatch.Await
  val Duration          = akka.util.Duration
  val ExecutionContext  = akka.dispatch.ExecutionContext
  val Future            = akka.dispatch.Future
  val Promise           = akka.dispatch.Promise
  val Timeout           = akka.util.Timeout

  // scalaz encapsulation
  type ScalazOrder[A]       = scalaz.Order[A]
  type ScalazOrdering       = scalaz.Ordering
  val ScalazOrder           = scalaz.Order
  val ScalazOrdering        = scalaz.Ordering
  type ScalaMathOrdering[A] = scala.math.Ordering[A]

  // scala stdlib encapsulation
  type spec           = scala.specialized
  type tailrec        = scala.annotation.tailrec
  type switch         = scala.annotation.switch
  type ArrayBuffer[A] = scala.collection.mutable.ArrayBuffer[A]
  type ListBuffer[A]  = scala.collection.mutable.ListBuffer[A]

  // java stdlib encapsulation
  type File        = java.io.File
  type IOException = java.io.IOException
  type ByteBuffer  = java.nio.ByteBuffer

  // joda encapsulation
  type DateTime = org.joda.time.DateTime
  type Instant  = org.joda.time.Instant
  type Period   = org.joda.time.Period

  // Can't overload in package objects in scala 2.9!
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                         = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap2(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)

  def abort(msg: String): Nothing = throw new RuntimeException(msg)

  def decimal(d: String) = BigDecimal(d, java.math.MathContext.UNLIMITED)

  final class StringExtensions(s: String) {
    def cpath = CPath(s)
  }

  implicit def stringExtensions(s: String) = new StringExtensions(s)
}

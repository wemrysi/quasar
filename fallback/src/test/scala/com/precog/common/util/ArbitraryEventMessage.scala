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
package com.precog.common
package ingest

import security._
import blueeyes._
import quasar.precog.JsonTestSupport._, Gen._

trait ArbitraryEventMessage {
  def genStreamId: Gen[Option[UUID]] = Gen.oneOf(Gen lzy Some(randomUuid), Gen const None)

  def genPath: Gen[Path] = Gen.resize(10, Gen.containerOf[List, String](alphaStr)) map { elements =>
    Path(elements.filter(_.length > 0))
  }

  def genWriteMode: Gen[WriteMode] =
      Gen.oneOf(AccessMode.Create, AccessMode.Replace, AccessMode.Append)

  def genStreamRef: Gen[StreamRef] =
    for {
      terminal <- arbitrary[Boolean]
      storeMode <- genWriteMode
    } yield StreamRef.forWriteMode(storeMode, terminal)

  def genEventId: Gen[EventId] =
    for {
      producerId <- choose(0,1000000)
      sequenceId <- choose(0, 1000000)
    } yield EventId(producerId, sequenceId)

  def genRandomIngest: Gen[Ingest] =
    for {
      apiKey <- alphaStr
      path <- genPath
      ownerAccountId <- alphaStr
      content <- vectorOf(genJValue)
      if !content.isEmpty
      jobId <- genIdentifier.optional
      streamRef <- genStreamRef
    } yield Ingest(apiKey, path, Some(Authorities(ownerAccountId)), content, jobId, instant.now(), streamRef)

  def genRandomArchive: Gen[Archive] =
    for {
      apiKey <- alphaStr
      path <- genPath
      jobId <- genIdentifier.optional
    } yield Archive(apiKey, path, jobId, instant.now())

  def genRandomIngestMessage: Gen[IngestMessage] =
    for {
      ingest <- genRandomIngest if ingest.writeAs.isDefined
      eventIds <- containerOfN[List, EventId](ingest.data.size, genEventId).map(l => Vector(l: _*))
      streamRef <- genStreamRef
    } yield {
      //TODO: Replace with IngestMessage.fromIngest when it's usable
      val data = (eventIds zip ingest.data) map { Function.tupled(IngestRecord.apply) }
      IngestMessage(ingest.apiKey, ingest.path, ingest.writeAs.get, data, ingest.jobId, instant.now(), streamRef)
    }

  def genRandomArchiveMessage: Gen[ArchiveMessage] =
    for {
      eventId <- genEventId
      archive <- genRandomArchive
    } yield ArchiveMessage(archive.apiKey, archive.path, archive.jobId, eventId, archive.timestamp)

  def genRandomEventMessage: Gen[EventMessage] =
    frequency(
      (1, genRandomArchiveMessage),
      (10, genRandomIngestMessage)
    )
}

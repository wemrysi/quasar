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
package security
package service

import com.precog.common.accounts._

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._

object v1 {
  case class GrantDetails(grantId: GrantId,
                          name: Option[String],
                          description: Option[String],
                          permissions: Set[Permission],
                          createdAt: Instant,
                          expirationDate: Option[DateTime]) {
    def isValidAt(timestamp: Instant) = {
      createdAt.isBefore(timestamp) && expirationDate.forall(_ isAfter (dateTime fromMillis timestamp.getMillis))
    }
  }
  object GrantDetails {
    val schema = "grantId" :: "name" :: "description" :: "permissions" :: ("createdAt" ||| instant.zero) :: "expirationDate" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[GrantDetails](schema)
  }

  case class APIKeyDetails(apiKey: APIKey, name: Option[String], description: Option[String], grants: Set[GrantDetails], issuerChain: List[APIKey])
  object APIKeyDetails {
    val schema = "apiKey" :: "name" :: "description" :: "grants" :: "issuerChain" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[APIKeyDetails](schema)
  }

  case class NewGrantRequest(name: Option[String],
                             description: Option[String],
                             parentIds: Set[GrantId],
                             permissions: Set[Permission],
                             expirationDate: Option[DateTime]) {
    def isExpired(at: Option[DateTime]) = (expirationDate, at) match {
      case (None, _)                 => false
      case (_, None)                 => true
      case (Some(expiry), Some(ref)) => expiry.isBefore(ref)
    }
  }

  object NewGrantRequest {
    private implicit val reqPermDecomposer = Permission.decomposerV1Base

    val schemaV1 = "name" :: "description" :: ("parentIds" ||| Set.empty[GrantId]) :: "permissions" :: "expirationDate" :: HNil

    implicit val decomposerV1 = IsoSerialization.decomposer[NewGrantRequest](schemaV1)
    implicit val extractorV1  = IsoSerialization.extractor[NewGrantRequest](schemaV1)
  }

  case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

  object NewAPIKeyRequest {
    val schemaV1 = "name" :: "description" :: "grants" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[NewAPIKeyRequest](schemaV1)
  }
}

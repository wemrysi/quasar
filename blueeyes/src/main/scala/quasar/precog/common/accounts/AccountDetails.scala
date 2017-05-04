package quasar.precog.common
package accounts

import quasar.precog.common.security.APIKey

import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._

case class AccountDetails(accountId: AccountId,
                          email: String,
                          accountCreationDate: DateTime,
                          apiKey: APIKey,
                          rootPath: Path,
                          plan: AccountPlan,
                          lastPasswordChangeTime: Option[DateTime] = None,
                          profile: Option[JValue] = None)

object AccountDetails {
  def from(account: Account): AccountDetails = {
    import account._
    AccountDetails(accountId, email, accountCreationDate, apiKey, rootPath, plan, lastPasswordChangeTime, profile)
  }

  val schema = "accountId" :: "email" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "lastPasswordChangeTime" :: "profile" :: HNil

  implicit val accountDetailsDecomposer = decomposerV[AccountDetails](schema, None)
  implicit val accountDetailsExtractor  = extractorV[AccountDetails](schema, None)
}

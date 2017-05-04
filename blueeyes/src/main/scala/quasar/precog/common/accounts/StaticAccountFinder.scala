package quasar.precog.common
package accounts

import org.slf4s.Logging

import scalaz.Monad
import scalaz.syntax.monad._

import quasar.blueeyes._
import quasar.precog.common.security._

class StaticAccountFinder[M[+ _]: Monad](accountId: AccountId, apiKey: APIKey, rootPath: Option[String] = None, email: String = "static@precog.com")
    extends AccountFinder[M]
    with Logging {
  private[this] val details = Some(AccountDetails(accountId, email, dateTime.zero, apiKey, Path(rootPath.getOrElse("/" + accountId)), AccountPlan.Root))

  log.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(details.get))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = details.point[M]
}

package quasar.precog.common.security

import quasar.precog.common._, accounts._
import quasar.precog.common.security.service._
import quasar.precog.TestSupport._

import scalaz._, Scalaz._

trait APIKeyManagerSpec[M[+_]] extends Specification {
  implicit def M: Monad[M] with Comonad[M]

  "API Key Manager" should {
    "properly ascribe parentage for grants" in {
      val path = Path("/user1/")
      val mgr = new InMemoryAPIKeyManager[M](quasar.blueeyes.util.Clock.System)

      val grantParentage = for {
        rootKey <- mgr.rootAPIKey
        rootGrantId <- mgr.rootGrantId
        perms = Account.newAccountPermissions("012345", Path("/012345/"))
        grantRequest = v1.NewGrantRequest(Some("testGrant"), None, Set(rootGrantId), perms, None)
        record <- mgr.newAPIKeyWithGrants(Some("test"), None, rootKey, Set(grantRequest))
        grants <- record.toList.flatMap(_.grants).map(mgr.findGrant).sequence
      } yield {
        (grants.flatten.flatMap(_.parentIds), rootGrantId)
      }

      val (grantParents, rootGrantId) = grantParentage.copoint

      val gs = grantParents collect { case gid: GrantId => gid }
      grantParents must haveSize(1)
      grantParents must contain(rootGrantId)
    }
  }
}

object APIKeyManagerSpec extends APIKeyManagerSpec[Need] {
  val M = Need.need
}


package quasar.precog.common

import quasar.blueeyes._, json._, serialization._
import quasar.blueeyes.json.serialization.Extractor._

import scalaz._
import scalaz.syntax.bifunctor._

package object security {
  type APIKey  = String
  type GrantId = String
}

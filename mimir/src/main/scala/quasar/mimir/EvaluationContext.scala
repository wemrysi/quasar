
package quasar.mimir

import quasar.yggdrasil._
import quasar.blueeyes._, json._, serialization._
import IsoSerialization._, Iso8601Serialization._, Versioned._
import quasar.precog.common._, security._, accounts._

final case class EvaluationContext(apiKey: APIKey, account: AccountDetails, basePath: Path, scriptPath: Path, startTime: DateTime)

object EvaluationContext {
  val schemaV1 = "apiKey" :: "account" :: "basePath" :: "scriptPath" :: "startTime" :: HNil

  implicit val decomposer: Decomposer[EvaluationContext] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor: Extractor[EvaluationContext]   = extractorV(schemaV1, Some("1.0".v))
}

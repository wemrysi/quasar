package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._
import scalaz._

case class PathMetadata(path: Path, pathType: PathMetadata.PathType)

object PathMetadata {
  sealed trait PathType
  case class DataDir(contentType: MimeType)  extends PathType
  case class DataOnly(contentType: MimeType) extends PathType
  case object PathOnly extends PathType
}

case class PathStructure(types: Map[CType, Long], children: Set[CPath])

object PathStructure {
  val Empty = PathStructure(Map.empty, Set.empty)
}

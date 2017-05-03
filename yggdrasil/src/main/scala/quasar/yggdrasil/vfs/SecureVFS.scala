package quasar.yggdrasil
package vfs

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.common.security._
import scalaz._, Scalaz._

trait VFSMetadata[M[+ _]] {
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[PathMetadata]]
  def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure]
  def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long]
}

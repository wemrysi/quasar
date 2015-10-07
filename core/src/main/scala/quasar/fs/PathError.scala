package quasar
package fs

import quasar.Predef.String

import pathy.Path._

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2 {
  def message: String
}

object PathError2 {
  final case class PathExistsError(path: pathy.Path[Rel, _, Sandboxed]) extends PathError2 {
    def message = posixCodec.printPath(path) + ": already exists"
  }

  final case class PathMissingError(path: pathy.Path[Rel, _, Sandboxed]) extends PathError2 {
    def message = posixCodec.printPath(path) + ": doesn't exist"
  }

  def PathExistsError[T]: pathy.Path[Rel, T, Sandboxed] => PathError2 =
    PathExistsError(_)

  def PathMissingError[T]: pathy.Path[Rel, T, Sandboxed] => PathError2 =
    PathMissingError(_)
}


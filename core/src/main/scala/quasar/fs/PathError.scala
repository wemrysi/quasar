package quasar
package fs

import quasar.Predef._
import quasar.fp._

import scalaz._
import pathy.Path._

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2 {
  import PathError2._

  def fold[X](
    pathExists: RelPath[Sandboxed] => X,
    pathNotFound: RelPath[Sandboxed] => X,
    invalidPath: (RelPath[Sandboxed], String) => X
  ): X =
    this match {
      case PathExists0(p)     => pathExists(p)
      case PathNotFound0(p)   => pathNotFound(p)
      case InvalidPath0(p, r) => invalidPath(p, r)
    }
}

object PathError2 {
  private final case class PathExists0(path: RelPath[Sandboxed])
    extends PathError2
  private final case class PathNotFound0(path: RelPath[Sandboxed])
    extends PathError2
  private final case class InvalidPath0(path: RelPath[Sandboxed], reason: String)
    extends PathError2

  val PathExists: RelPath[Sandboxed] => PathError2 =
    PathExists0(_)

  val DirExists: RelDir[Sandboxed] => PathError2 =
    PathExists compose \/.left

  val FileExists: RelFile[Sandboxed] => PathError2 =
    PathExists compose \/.right

  val PathNotFound: RelPath[Sandboxed] => PathError2 =
    PathNotFound0(_)

  val DirNotFound: RelDir[Sandboxed] => PathError2 =
    PathNotFound compose \/.left

  val FileNotFound: RelFile[Sandboxed] => PathError2 =
    PathNotFound compose \/.right

  val InvalidPath: (RelPath[Sandboxed], String) => PathError2 =
    InvalidPath0(_, _)

  implicit val pathErrorShow: Show[PathError2] = {
    val typeStr: RelPath[Sandboxed] => String =
      _.fold(κ("Dir"), κ("File"))

    Show.shows(_.fold(
      p => s"${typeStr(p)} already exists: " +
           p.fold(posixCodec.printPath, posixCodec.printPath),

      p => s"${typeStr(p)} not found: " +
           p.fold(posixCodec.printPath, posixCodec.printPath),

      (p, r) => s"${typeStr(p)} " +
           p.fold(posixCodec.printPath, posixCodec.printPath) +
           s" is invalid: $r"))
  }
}


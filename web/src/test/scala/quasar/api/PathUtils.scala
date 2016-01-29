package quasar.api

import quasar.Predef._

import pathy.Path, Path._
import scalaz._, Scalaz._

trait PathUtils {
  // NB: these paths confuse the codec and don't seem important at the moment.
  // See https://github.com/slamdata/scala-pathy/issues/23.
  def hasDot(p: Path[_, _, _]): Boolean =
    flatten(false, false, false, d => d == "." || d == "..", f => f == "." || f == "..", p).toList.contains(true)
}

object PathUtils extends PathUtils

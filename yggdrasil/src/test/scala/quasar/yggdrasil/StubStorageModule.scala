package quasar.yggdrasil

import quasar.blueeyes._
import quasar.precog.common._
import scalaz._

trait StubProjectionModule[M[+_], Block] extends ProjectionModule[M, Block] { self =>
  implicit def M: Monad[M]

  protected def projections: Map[Path, Projection]

  class ProjectionCompanion extends ProjectionCompanionLike[M] {
    def apply(path: Path) = M.point(projections.get(path))
  }
}

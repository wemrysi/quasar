package quasar

import scalaz.{\/, Coyoneda, EitherT}
import pathy.Path._

package object fs {
  type ReadFileF[A]   = Coyoneda[ReadFile, A]
  type WriteFileF[A]  = Coyoneda[WriteFile, A]
  type FileSystemF[A] = Coyoneda[FileSystem, A]

  type RelPath[S] = RelDir[S] \/ RelFile[S]

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]
}


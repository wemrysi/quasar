package quasar

import scalaz.{Coyoneda, EitherT}

package object fs {
  type ReadFileF[A] = Coyoneda[ReadFile, A]
  type WriteFileF[A] = Coyoneda[WriteFile, A]

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]
}


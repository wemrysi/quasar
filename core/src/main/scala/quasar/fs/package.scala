package quasar

import quasar.fp._
import scalaz._
import pathy.Path._

package object fs {
  type ReadFileF[A]   = Coyoneda[ReadFile, A]
  type WriteFileF[A]  = Coyoneda[WriteFile, A]
  type FileSystemF[A] = Coyoneda[FileSystem, A]

  type FS0[A] = Coproduct[WriteFileF, FileSystemF, A]
  type FS[A]  = Coproduct[ReadFileF, FS0, A]

  type RelPath[S] = RelDir[S] \/ RelFile[S]

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]

  def interpretFS[M[_]: Functor](r: ReadFile ~> M, w: WriteFile ~> M, f: FileSystem ~> M): FS ~> M =
    interpret.interpret3[ReadFileF, WriteFileF, FileSystemF, M](
      Coyoneda.liftTF(r), Coyoneda.liftTF(w), Coyoneda.liftTF(f))
}


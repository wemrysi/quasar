package quasar
package fs

import quasar.fp._

import pathy.{Path => PPath}, PPath._

import scalaz._

object chroot {

  /** Rebases all paths in [[ReadFile]] operations onto the given prefix. */
  def readFile(prefix: AbsDir[Sandboxed]): ReadFile ~> ReadFile =
    new (ReadFile ~> ReadFile) {
      import ReadFile._
      def apply[A](rf: ReadFile[A]) = rf match {
        case Open(f, off, lim) => Open(rebase(f, prefix), off, lim)
        case _                 => rf
      }
    }

  def readFileF[S[_]](prefix: AbsDir[Sandboxed])(implicit S: ReadFileF :<: S): S ~> S =
    interpret.injectedNT[ReadFileF, S](Coyoneda liftT readFile(prefix))

  /** Rebases all paths in [[WriteFile]] operations onto the given prefix. */
  def writeFile(prefix: AbsDir[Sandboxed]): WriteFile ~> WriteFile =
    new (WriteFile ~> WriteFile) {
      import WriteFile._
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(f) => Open(rebase(f, prefix))
        case _       => wf
      }
    }

  def writeFileF[S[_]](prefix: AbsDir[Sandboxed])(implicit S: WriteFileF :<: S): S ~> S =
    interpret.injectedNT[WriteFileF, S](Coyoneda liftT writeFile(prefix))

  /** Rebases all paths in [[ManageFile]] operations onto the given prefix. */
  def manageFile(prefix: AbsDir[Sandboxed]): ManageFile ~> ManageFile =
    new (ManageFile ~> ManageFile) {
      import ManageFile._, MoveScenario._
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scn, sem) =>
          Move(
            scn.fold(
              (src, dst) => DirToDir(rebase(src, prefix), rebase(dst, prefix)),
              (src, dst) => FileToFile(rebase(src, prefix), rebase(dst, prefix))),
            sem)

        case Delete(p) =>
          Delete(p.bimap(rebase(_, prefix), rebase(_, prefix)))

        case ListContents(d) =>
          ListContents(rebase(d, prefix))

        case TempFile(nt) =>
          TempFile(nt map (rebase(_, prefix)))
      }
    }

  def manageFileF[S[_]](prefix: AbsDir[Sandboxed])(implicit S: ManageFileF :<: S): S ~> S =
    interpret.injectedNT[ManageFileF, S](Coyoneda liftT manageFile(prefix))

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def fileSystem[S[_]](prefix: AbsDir[Sandboxed])
                      (implicit S0: ReadFileF :<: S, S1: WriteFileF :<: S, S2: ManageFileF :<: S): S ~> S = {

    readFileF[S](prefix) compose writeFileF[S](prefix) compose manageFileF[S](prefix)
  }

  ////

  // TODO: AbsDir relativeTo rootDir doesn't need to be partial, add the appropriate method to pathy
  private def rebase[T](p: PPath[Abs,T,Sandboxed], onto: AbsDir[Sandboxed]): PPath[Abs,T,Sandboxed] =
    p.relativeTo(rootDir[Sandboxed]).fold(p)(onto </> _)
}

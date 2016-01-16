package quasar.fs

import quasar.Predef._

import pathy.Path._
import scalaz._, Scalaz._

/**
  * Use with care. Functions make the assumption that Sandboxed Pathy paths do not contain ParentIn or Current.
  * This can not currently be guaranteed.
  */
object SandboxedPathy {

  implicit val fileNameEqual: Equal[FileName] = Equal.equalA
  implicit val dirNameEqual: Equal[DirName] = Equal.equalA

  def rootSubPath(depth: Int, p: APath): APath = {
    val elems = flatten(none, none, none, DirName(_).some, FileName(_).some, p).toList.unite
    val dirs = elems.collect { case e: DirName => e }
    val file = elems.collect { case e: FileName => e }.headOption

    def dirsPath(dirs: List[DirName]) = dirs.foldLeft(rootDir){ case (a, e) => a </> dir1(e) }

    if (depth > dirs.size) {
      val p = dirsPath(dirs)
      file.cata(p </> file1(_), p)
    }
    else
      dirsPath(dirs.take(depth))
  }

  def largestCommonPathFromRoot(a: APath, b: APath): APath = {
    val i = (0 until (depth(a) max depth(b))).find(i => segAt(i, a) =/= segAt(i, b))

    rootSubPath(i.getOrElse(0), a)
  }

  def segAt[B,T,S](index: Int, path: pathy.Path[B,T,S]): Option[FileName \/ DirName] = {
    scala.Predef.require(index >= 0)
    val list =
      pathy.Path.flatten(none, none, none, DirName(_).right.some,FileName(_).left.some,path).toIList.unite
    list.drop(index).headOption
  }

}

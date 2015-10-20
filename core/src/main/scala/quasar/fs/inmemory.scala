package quasar
package fs

import quasar.Predef._

import scalaz._, Scalaz._
import pathy.Path._

/** In-Memory Read/Write/ManageFile interpreters, useful for testing/stubbing
  * when a "real" interpreter isn't needed or desired.
  *
  * NB: Since this is in-memory, careful with writing large amounts of data to
  *     the file system.
  */
object inmemory {
  import ReadFile._, WriteFile._, ManageFile._, FileSystemError._, PathError2._

  type FM = Map[RelFile[Sandboxed], Vector[Data]]
  type RM = Map[ReadHandle, Reading]
  type WM = Map[WriteHandle, RelFile[Sandboxed]]

  type InMemoryFs[A]  = State[InMemState, A]
  type InMemStateR[A] = (InMemState, A)

  final case class Reading(f: RelFile[Sandboxed], start: Natural, lim: Option[Positive], pos: Int)

  final case class InMemState(seq: Long, fm: FM, rm: RM, wm: WM)

  object InMemState {
    val empty = InMemState(0, Map.empty, Map.empty, Map.empty)
  }

  val readFile: ReadFile ~> InMemoryFs = new (ReadFile ~> InMemoryFs) {
    def apply[A](rf: ReadFile[A]) = rf match {
      case ReadFile.Open(f, off, lim) =>
        fileL(f).st flatMap {
          case Some(_) =>
            for {
              i <- nextSeq
              h =  ReadHandle(i)
              _ <- readingL(h) := Reading(f, off, lim, 0).some
            } yield h.right

          case None =>
            fsFileNotFound(f)
        }

      case ReadFile.Read(h) =>
        readingL(h) flatMap {
          case Some(Reading(f, st, lim, pos)) =>
            fileL(f).st flatMap {
              case Some(xs) =>
                val rIdx =
                  st.toInt + pos

                val rCount =
                  rChunkSize                          min
                  lim.cata(_.toInt - pos, rChunkSize) min
                  (xs.length - rIdx)

                if (rCount <= 0)
                  Vector.empty.right.point[InMemoryFs]
                else
                  (rPosL(h) := (pos + rCount))
                    .map(_ => xs.slice(rIdx, rIdx + rCount).right)

              case None =>
                fsFileNotFound(f)
            }

          case None =>
            UnknownReadHandle(h).left.point[InMemoryFs]
        }

      case ReadFile.Close(h) =>
        rClose(h)
    }
  }

  val writeFile: WriteFile ~> InMemoryFs = new (WriteFile ~> InMemoryFs) {
    def apply[A](wf: WriteFile[A]) = wf match {
      case WriteFile.Open(f) =>
        for {
          i <- nextSeq
          h =  WriteHandle(i)
          _ <- wFileL(h) := Some(f)
        } yield h.right

      case WriteFile.Write(h, xs) =>
        wFileL(h).st flatMap {
          case Some(f) =>
            fileL(f) mods (_ map (_ ++ xs) orElse Some(xs)) as Vector.empty

          case None =>
            Vector(UnknownWriteHandle(h)).point[InMemoryFs]
        }

      case WriteFile.Close(h) =>
        (wFileL(h) := None).void
    }
  }

  val manageFile: ManageFile ~> InMemoryFs = new (ManageFile ~> InMemoryFs) {
    def apply[A](fsa: ManageFile[A]) = fsa match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))

      case Delete(path) =>
        path.fold(deleteDir, deleteFile)

      case ListContents(dir) =>
        fmL flatMap (
          _.keys.flatMap(_ relativeTo dir)
            .toList.toNel
            .cata(
              _.foldMap(s => Set(Node.File(dir </> s)))
                .right.point[InMemoryFs],
              fsDirNotFound(dir)))

      case TempFile(nearTo) =>
        nextSeq map (n => nearTo.cata(
          renameFile(_, _ => FileName(tmpName(n))),
          tmpDir </> file(tmpName(n))))
    }
  }

  ////

  private def tmpDir: RelDir[Sandboxed] = dir("__quasar") </> dir("tmp")
  private def tmpName(n: Long) = s"__quasar.gen_$n"

  private val seqL: InMemState @> Long =
    Lens.lensg(s => n => s.copy(seq = n), _.seq)

  private def nextSeq: InMemoryFs[Long] =
    seqL <%= (_ + 1)

  private val fmL: InMemState @> FM =
    Lens.lensg(s => m => s.copy(fm = m), _.fm)

  private def fileL(f: RelFile[Sandboxed]): InMemState @> Option[Vector[Data]] =
    Lens.mapVLens(f) <=< fmL

  //----

  /** Chunk size to use for [[Read]]s. */
  private val rChunkSize = 10

  private val rmL: InMemState @> RM =
    Lens.lensg(s => m => s.copy(rm = m), _.rm)

  private def readingL(h: ReadHandle): InMemState @> Option[Reading] =
    Lens.mapVLens(h) <=< rmL

  private val readingPosL: Reading @> Int =
    Lens.lensg(r => p => r.copy(pos = p), _.pos)

  private def rPosL(h: ReadHandle): InMemState @?> Int =
    ~readingL(h) >=> PLens.somePLens >=> ~readingPosL

  private def rClose(h: ReadHandle): InMemoryFs[Unit] =
    (readingL(h) := None).void

  //----

  private val wmL: InMemState @> WM =
    Lens.lensg(s => m => s.copy(wm = m), _.wm)

  private def wFileL(h: WriteHandle): InMemState @> Option[RelFile[Sandboxed]] =
    Lens.mapVLens(h) <=< wmL

  //----

  private def fsFileNotFound[A](f: RelFile[Sandboxed]): InMemoryFs[FileSystemError \/ A] =
    PathError(FileNotFound(f)).left.point[InMemoryFs]

  private def fsFileExists[A](f: RelFile[Sandboxed]): InMemoryFs[FileSystemError \/ A] =
    PathError(FileExists(f)).left.point[InMemoryFs]

  private def fsDirNotFound[A](d: RelDir[Sandboxed]): InMemoryFs[FileSystemError \/ A] =
    PathError(DirNotFound(d)).left.point[InMemoryFs]

  private def moveDir(src: RelDir[Sandboxed], dst: RelDir[Sandboxed], s: MoveSemantics): InMemoryFs[FileSystemError \/ Unit] =
    for {
      m     <- fmL.st
      sufxs =  m.keys.flatMap(_ relativeTo src).toStream
      files =  sufxs map (src </> _) zip (sufxs map (dst </> _))
      r0    <- files.traverseU { case (sf, df) => EitherT(moveFile(sf, df, s)) }.run
      r1    <- r0.fold(_.left.point[InMemoryFs],
                       xs => if (xs.isEmpty) fsDirNotFound(src)
                             else ().right.point[InMemoryFs])
    } yield r1

  private def moveFile(src: RelFile[Sandboxed], dst: RelFile[Sandboxed], s: MoveSemantics): InMemoryFs[FileSystemError \/ Unit] = {
    val move0: InMemoryFs[FileSystemError \/ Unit] = for {
      v <- fileL(src) <:= None
      r <- v.cata(
             xs => (fileL(dst) := Some(xs)) as ().right,
             fsFileNotFound(src))
    } yield r

    s.fold(
      move0,
      fileL(dst).st flatMap (_ ? fsFileExists[Unit](dst) | move0),
      fileL(dst).st flatMap (_ ? move0 | fsFileNotFound(dst)))
  }

  private def deleteDir(d: RelDir[Sandboxed]): InMemoryFs[FileSystemError \/ Unit] =
    for {
      m  <- fmL.st
      ss =  m.keys.flatMap(_ relativeTo d).toStream
      r0 <- ss.traverseU(f => EitherT(deleteFile(f))).run
      r1 <- r0.fold(_.left.point[InMemoryFs],
                    xs => if (xs.isEmpty) fsDirNotFound(d)
                          else ().right.point[InMemoryFs])
    } yield r1

  private def deleteFile(f: RelFile[Sandboxed]): InMemoryFs[FileSystemError \/ Unit] =
    (fileL(f) <:= None)
      .flatMap(_ ? ().right[FileSystemError].point[InMemoryFs] | fsFileNotFound(f))
}

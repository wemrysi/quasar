package quasar
package fs

import quasar.Predef._

import scalaz._, Scalaz._
import pathy.Path._

/** In-Memory Read/Write/FileSystem interpreters, useful for testing/stubbing
  * when a "real" interpreter isn't needed or desired.
  *
  * NB: Since this is in-memory, careful with writing large amounts of data to
  *     the file system.
  */
object inmemory {
  import ReadFile._, WriteFile._, FileSystem._

  type FS = Map[RelFile[Sandboxed], Vector[Data]]
  type RH = Map[ReadHandle, Reading]
  type WH = Map[WriteHandle, RelFile[Sandboxed]]

  type InMemoryFs[A]  = State[InMemState, A]
  type InMemStateR[A] = (InMemState, A)

  final case class Reading(f: RelFile[Sandboxed], start: Natural, lim: Option[Positive], pos: Int)
  final case class InMemState(seq: Long, fs: FS, rh: RH, wh: WH)

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
            rNotFound(f)
        }

      case ReadFile.Read(h) =>
        readingL(h) flatMap {
          case Some(Reading(f, st, lim, pos)) =>
            fileL(f).st flatMap {
              case Some(xs) =>
                val rCount =
                  rChunkSize                          min
                  lim.cata(_.toInt - pos, rChunkSize) min
                  (xs.length - (st.toInt + pos))

                if (rCount <= 0)
                  rClose(h) as Vector.empty.right
                else
                  (rPosL(h) := (pos + rCount))
                    .map(_ => xs.slice(st.toInt, st.toInt + rCount).right)

              case None =>
                rNotFound(f)
            }

          case None =>
            ReadError.UnknownHandle(h).left.point[InMemoryFs]
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
            Vector(WriteError.UnknownHandle(h)).point[InMemoryFs]
        }

      case WriteFile.Close(h) =>
        (wFileL(h) := None).void
    }
  }

  val fileSystem: FileSystem ~> InMemoryFs = new (FileSystem ~> InMemoryFs) {
    def apply[A](fsa: FileSystem[A]) = fsa match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))

      case Delete(path) =>
        path.fold(deleteDir, deleteFile)

      case ListContents(dir) =>
        fsL flatMap (
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

  /** Pure interpreter that interprets InMemoryFs into a tuple of the
    * final state and value produced.
    */
  val run: InMemoryFs ~> InMemStateR = new (InMemoryFs ~> InMemStateR) {
    def apply[A](imfs: InMemoryFs[A]) =
      imfs.run(InMemState(0, Map.empty, Map.empty, Map.empty))
  }

  ////

  private def tmpDir: RelDir[Sandboxed] = dir("__quasar") </> dir("tmp")
  private def tmpName(n: Long) = s"__quasar.gen_$n"

  private val seqL: InMemState @> Long =
    Lens.lensg(s => n => s.copy(seq = n), _.seq)

  private def nextSeq: InMemoryFs[Long] =
    seqL <%= (_ + 1)

  private val fsL: InMemState @> FS =
    Lens.lensg(s => m => s.copy(fs = m), _.fs)

  private def fileL(f: RelFile[Sandboxed]): InMemState @> Option[Vector[Data]] =
    Lens.mapVLens(f) <=< fsL

  //----

  /** Chunk size to use for [[Read]]s. */
  private val rChunkSize = 10

  private def rNotFound[A](f: RelFile[Sandboxed]): InMemoryFs[ReadError \/ A] =
    ReadError.PathError(PathError2.FileNotFound(f)).left.point[InMemoryFs]

  private val rhL: InMemState @> RH =
    Lens.lensg(s => m => s.copy(rh = m), _.rh)

  private def readingL(h: ReadHandle): InMemState @> Option[Reading] =
    Lens.mapVLens(h) <=< rhL

  private val readingPosL: Reading @> Int =
    Lens.lensg(r => p => r.copy(pos = p), _.pos)

  private def rPosL(h: ReadHandle): InMemState @?> Int =
    ~readingL(h) >=> PLens.somePLens >=> ~readingPosL

  private def rClose(h: ReadHandle): InMemoryFs[Unit] =
    (readingL(h) := None).void

  //----

  private val whL: InMemState @> WH =
    Lens.lensg(s => m => s.copy(wh = m), _.wh)

  private def wFileL(h: WriteHandle): InMemState @> Option[RelFile[Sandboxed]] =
    Lens.mapVLens(h) <=< whL

  //----

  private def fsFileNotFound[A](f: RelFile[Sandboxed]): InMemoryFs[PathError2 \/ A] =
    PathError2.FileNotFound(f).left.point[InMemoryFs]

  private def fsFileExists[A](f: RelFile[Sandboxed]): InMemoryFs[PathError2 \/ A] =
    PathError2.FileExists(f).left.point[InMemoryFs]

  private def fsDirNotFound[A](d: RelDir[Sandboxed]): InMemoryFs[PathError2 \/ A] =
    PathError2.DirNotFound(d).left.point[InMemoryFs]

  private def moveDir(src: RelDir[Sandboxed], dst: RelDir[Sandboxed], s: MoveSemantics): InMemoryFs[PathError2 \/ Unit] =
    for {
      m     <- fsL.st
      sufxs =  m.keys.flatMap(_ relativeTo src).toStream
      files =  sufxs map (src </> _) zip (sufxs map (dst </> _))
      r0    <- files.traverseU { case (sf, df) => EitherT(moveFile(sf, df, s)) }.run
      r1    <- r0.fold(_.left.point[InMemoryFs],
                       xs => if (xs.isEmpty) fsDirNotFound(src)
                             else ().right.point[InMemoryFs])
    } yield r1

  private def moveFile(src: RelFile[Sandboxed], dst: RelFile[Sandboxed], s: MoveSemantics): InMemoryFs[PathError2 \/ Unit] = {
    val move0: InMemoryFs[PathError2 \/ Unit] = for {
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

  private def deleteDir(d: RelDir[Sandboxed]): InMemoryFs[PathError2 \/ Unit] =
    for {
      m  <- fsL.st
      ss =  m.keys.flatMap(_ relativeTo d).toStream
      r0 <- ss.traverseU(f => EitherT(deleteFile(f))).run
      r1 <- r0.fold(_.left.point[InMemoryFs],
                    xs => if (xs.isEmpty) fsDirNotFound(d)
                          else ().right.point[InMemoryFs])
    } yield r1

  private def deleteFile(f: RelFile[Sandboxed]): InMemoryFs[PathError2 \/ Unit] =
    (fileL(f) <:= None)
      .flatMap(_ ? ().right[PathError2].point[InMemoryFs] | fsFileNotFound(f))
}

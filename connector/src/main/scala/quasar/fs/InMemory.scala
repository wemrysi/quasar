/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.fs

import quasar.Predef._
import quasar.{Data, PhaseResult, PhaseResults}
import quasar.contrib.pathy._
import quasar.fp._
import numeric._
import quasar.Optimizer

import matryoshka.{Fix, Recursive}, Recursive.ops._
import pathy.Path._
import scalaz._, Scalaz._
import shapeless._
import scala.math.min
import Lens.{ lensg, mapVLens }
import ReadFile._, WriteFile._, ManageFile._, QueryFile._
import FileSystemError._, PathError._
import FileSystemIndependentTypes._
import InMemory._

/**
 * TODO: Use KeyValueStore.
 */
object InMemory extends StatefulFileSystem {
  val FsType     = FileSystemType("in-memory")
  val rChunkSize = 10 // Chunk size to use for [[Read]]s.
  val fileSystem = Impl.fileSystem

  type S              = InMemState
  type InMemoryFs[A]  = F[A]
  type InMemStateR[A] = InMemState -> A
  type PlanMap[+V]    = Map[FixPlan, V]

  /** Represents the current state of the InMemoryFilesystem
    * @param seq Represents the next available uid for a ReadHandle or WriteHandle
    * @param contents The mapping of Data associated with each file in the Filesystem
    * @param rm Currently open [[quasar.fs.ReadFile.ReadHandle]]s
    * @param wm Currently open [[quasar.fs.WriteFile.WriteHandle]]s
    */
  final case class InMemState(
    nextUid: Long,
    fileMap: FileMap[Chunks],
    readMap: ReadMap[Reading],
    writeMap: WriteMap[AFile],
    planMap: PlanMap[Chunks],
    queryMap: QueryMap[Chunks]
  ) {
    def contents: FileMap[Chunks]     = fileMap
    def contains(f: AFile): Boolean   = fileMap contains f
    def get(f: AFile): Option[Chunks] = fileMap get f
  }
  object InMemState {
    val empty = InMemState(0, Map(), Map(), Map(), Map(), Map())
    def fromFiles(files: FileMap[Chunks]): InMemState = empty.copy(fileMap = files)
  }
  final case class Reading(f: AFile, start: Natural, lim: Option[Positive], pos: Int)

  val fileMapL: StateLens[FileMap[Chunks]] = lensg(s => m => s.copy(fileMap = m), _.fileMap)
  val planMapL: StateLens[PlanMap[Chunks]] = lensg(s => m => s.copy(planMap = m), _.planMap)
  val nextUidL: StateLens[Long]            = lensg(s => n => s.copy(nextUid = n), _.nextUid)
  val readingPosL: Reading @> Int          = lensg(r => p => r.copy(pos = p), _.pos)

  def fileL(f: AFile): StateLensP[Chunks]       = fileMapL >=> mapVLens(f)
  def readingL(h: RHandle): StateLensP[Reading] = mapVLens(h) <=< lensg(s => m => s.copy(readMap = m), _.readMap)
  def resultL(h: QHandle): StateLensP[Chunks]   = mapVLens(h) <=< lensg(s => m => s.copy(queryMap = m), _.queryMap)
  def wFileL(h: WHandle): StateLensP[AFile]     = mapVLens(h) <=< lensg(s => m => s.copy(writeMap = m), _.writeMap)
  def rPosL(h: ReadHandle): InMemState @?> Int  = ~readingL(h) >=> PLens.somePLens >=> ~readingPosL

  object Impl extends InMemoryFsImpl
}

/** In-Memory FileSystem interpreters, useful for testing/stubbing
  * when a "real" interpreter isn't needed or desired.
  *
  * NB: Since this is in-memory, careful with writing large amounts of data to
  *     the file system.
  */
sealed class InMemoryFsImpl extends UnifiedFileSystem[InMemoryFs] {
  private def deleteQ(h: QHandle): FOpt[Chunks]             = resultL(h) <:= none
  private def deleteR(h: RHandle): FOpt[Reading]            = readingL(h) <:= none
  private def deleteW(h: WHandle): FOpt[AFile]              = wFileL(h) <:= none
  private def deleteF(f: AFile): FOpt[Chunks]               = fileL(f) <:= none
  private def replaceKey(f: AFile, v: Chunks): FOpt[Chunks] = fileL(f) <:= some(v)
  private def setKey(f: AFile, v: Chunks): FOpt[Chunks]     = fileL(f) := some(v)

  implicit class OptionsOps[A](private val x: Option[A]) {
    def |(err: => FileSystemError): LR[A]  = x toRightDisjunction err
    def or(err: => FileSystemError): LR[A] = this | err
  }

  def list(dir: ADir): FLR[DirList] =
    ls(dir) map (r => if (dir === rootDir) \/-(r getOrElse makeDirList()) else r )

  def more(h: ResultHandle): FLR[Chunks] = resultL(h) flatMap {
    case Some(xs) => (resultL(h) := some(Vector())) as xs
    case _        => unknownResultHandle(h)
  }

  def closeQ(h: QHandle): F[Unit]     = deleteQ(h).void
  def exists(file: AFile): F[Boolean] = onState(_.fileMap contains file)

  def evaluate(lp: FixPlan): FPLR[ResultHandle] = evalPlan(lp) { data =>
    for {
      h <- nextSeq ∘ (ResultHandle(_))
      _ <- resultL(h) := some(data)
    } yield h
  }

  private def simpleEvaluation(lp0: FixPlan): ER[Chunks] = {
    import quasar.LogicalPlan._
    import quasar.std.StdLib.set.{Drop, Take}
    import quasar.std.StdLib.identity.Squash

    val optLp = Optimizer.optimize(lp0)

    EitherT(
      onState(mem =>
        optLp.para[LR[Chunks]] {
          case ReadF(path) =>
            // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
            val aPath = mkAbsolute(rootDir, path)
            fileL(aPath) get mem or unknownPath(aPath)

          case InvokeFUnapply(Drop, Sized((_,src), (Fix(ConstantF(Data.Int(skip))),_))) =>
            (src ⊛ skip.safeToInt.toRightDisjunction(unknownPlan(optLp)))(_.drop(_))

          case InvokeFUnapply(Take, Sized((_,src), (Fix(ConstantF(Data.Int(limit))),_))) =>
            (src ⊛ limit.safeToInt.toRightDisjunction(unknownPlan(optLp)))(_.take(_))

          case InvokeFUnapply(Squash, Sized((_,src))) => src
          case ConstantF(data)                        => Vector(data)
          case other                                  =>
            (planMapL get mem mapKeys Optimizer.optimize get Fix(other map (_._1))) or unknownPlan(optLp)
        }
      )
    )
  }

  private def evalPlan[A](lp: FixPlan)(f: Chunks => F[A]): FPLR[A] =
    phaseResults(lp) tuple (simpleEvaluation(lp) flatMap (x => EitherT right f(x))).run

  private def executionPlan(lp: FixPlan, queries: PlanMap[Chunks]): ExecutionPlan =
    ExecutionPlan(FsType, s"Lookup $lp in $queries")

  private def phaseResults(lp: FixPlan): F[PhaseResults] =
    onState(s => Vector(PhaseResult.Detail("Lookup in Memory", executionPlan(lp, s.planMap).description)))

  def execute(lp: FixPlan, out: AFile): FPLR[AFile] =
    evalPlan(lp)(data => (fileL(out) := some(data)) as out)

  def explain(lp: FixPlan): FPLR[ExecutionPlan] =
    phaseResults(lp) tuple planMapL.st.map(executionPlan(lp, _))

  def openForRead(f: AFile, off: Natural, lim: Option[Positive]): FLR[ReadHandle] =
    fileL(f).st >>= (_.fold[State[InMemState, FileSystemError \/ ReadHandle]](
      unknownPath(f))(
      κ(for {
        h <- nextSeq ∘ (ReadHandle(f, _))
        _ <- readingL(h) := Reading(f, off, lim, 0).some
      } yield h.right)))

  def read(h: ReadHandle): FLR[Chunks] =
    readingL(h) flatMap {
      case None                          => unknownReadHandle(h)
      case Some(r @ Reading(f, _, _, _)) =>
        fileL(f).st flatMap {
          case Some(xs) => doRead(h, r, xs)
          case _        => unknownPath(f)
        }
    }

  def closeR(h: ReadHandle): F[Unit] = deleteR(h).void

  def openForWrite(f: AFile): FLR[WriteHandle] = {
    for {
      h <- nextSeq ∘ (WriteHandle(f, _))
      _ <- wFileL(h) := Some(f)
      _ <- fileL(f) %= (_ orElse Some(Vector()))
    } yield h.right
  }

  def write(h: WriteHandle, xs: Chunks): F[Errors] = wFileL(h).st flatMap {
    case Some(f) => fileL(f) mods (_ map (_ ++ xs) orElse xs.some) as Vector()
    case None    => Vector(unknownWriteHandle(h))
  }
  def closeW(h: WriteHandle): F[Unit] = deleteW(h).void

  import MoveSemantics._

  def deleteFile(f: AFile): FLR[Unit] = deleteF(f) map (_.fold[LR[Unit]](unknownPath(f))(_ => ()))

  def deleteDir(d: ADir): InMemoryFs[FileSystemError \/ Unit] =
    for {
      ss <- fileMapL.st ∘ (_.keys.toStream.map(_ relativeTo d).unite)
      r  <- ss.traverse(f => EitherT(deleteFile(d </> f))).run ∘
              (_ >>= (_.nonEmpty either (()) or unknownPath(d)))
    } yield r

  def moveDir(src: ADir, dst: ADir, s: MoveSemantics): FLR[Unit] =
    for {
      files <- fileMapL.st ∘
                 (_.keys.toStream.map(_ relativeTo src).unite ∘
                   (sufx => (src </> sufx, dst </> sufx)))
      r     <- files.traverse { case (sf, df) => EitherT(moveFile(sf, df, s)) }.run ∘
                 (_ >>= (_.nonEmpty either (()) or unknownPath(src)))
    } yield r

  def moveFile(src: AFile, dst: AFile, s: MoveSemantics): FLR[Unit] = {
    val move0: InMemoryFs[FileSystemError \/ Unit] =
      (fileL(src) <:= None) >>=
        (_.cata(xs => (fileL(dst) := Some(xs)) as ().right, unknownPath(src)))

    s match {
      case Overwrite     => move0
      case FailIfExists  => fileL(dst).st flatMap (_ ? (pathErr(pathExists(dst)): FLR[Unit]) | move0)
      case FailIfMissing => fileL(dst).st flatMap (_ ? move0 | unknownPath(dst))
    }
  }

  def deletePath(path: APath): FLR[Unit] = refineType(path).fold(deleteDir, deleteFile)
  def createTempFile(near: APath): FLR[AFile] = {
    nextSeq map (n => refineType(near).fold(
      _ </> file(tmpName(n)),
      renameFile(_, κ(FileName(tmpName(n))))
    ))
  }

  def doRead(h: ReadHandle, r: Reading, xs: Chunks): FLR[Chunks] = {
    val Reading(f, st, lim, pos) = r
    val rIdx    = st.get.toInt + pos
    val limCata = lim.cata(_.get.toInt - pos, rChunkSize)
    val rCount  = min(min(rChunkSize, limCata), xs.length - rIdx)
    if (rCount <= 0)
      Vector()
    else
      (rPosL(h) := (pos + rCount)) map κ(xs.slice(rIdx, rIdx + rCount))
  }
  def ls(d: ADir): FLR[DirList] =
    fileMapL.st map (
      _.keys.toList.map(_ relativeTo d).unite.toNel
        .map(_ foldMap (f => firstSegmentName(f).toSet))
        .toRightDisjunction(unknownPath(d)))

  def nextSeq: F[Long]         = nextUidL <%= (_ + 1)
  def tmpName(n: Long): String = s"__quasar.gen_$n"
}

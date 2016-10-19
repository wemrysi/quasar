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

package quasar.physical.jsonfile.fs

import quasar._
import quasar.Predef._
import quasar.fs._, PathError._
import quasar.fs.FileSystemError._
import quasar.effect._
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._
import Scalaz.{ ToIdOps => _, _ }
import FileSystemIndependentTypes._
import ManageFile.MoveSemantics._
import ManageFile.MoveScenario._
// import ygg.table._

class FsAlgebras[S[_]] extends STypes[S] {
  private def defaultChunkSize: Int = 10

  def emptyData(): Chunks                  = Vector()
  def ignoreRes[A](x: FS[A]): FLR[Unit] = x map (_ => ().right)

  implicit class ADirOps2(dir: ADir) {
    def /(name: DirName): ADir = dir </> dir1(name)
  }
  implicit class ADirOps1(dir: ADir) {
    def /(name: FileName): AFile = dir </> file1(name)
  }
  implicit class ADirOps(dir: ADir) {
    def /(name: String): AFile      = dir / FileName(name)
    def /(name: PathSegment): APath = name.fold(dir / _, dir / _)
  }
  implicit class APathOps(path: APath) {
    def name: Option[PathSegment]    = refineType(path).fold(x => dirName(x) map (_.left), x => Some(fileName(x)))
    def nameIfFile: Option[FileName] = name flatMap (_.toOption)
    def nameIfDir: Option[DirName]   = name flatMap (_.swap.toOption)

    def parentOrSelf: ADir = refineType(path).fold(x => x, fileParent _)
  }

  def ls(dir: ADir)(implicit KVF: KVFile[S]): FLR[DirList] = KVF.keys map (fs =>
    fs.map(_ relativeTo dir).unite.toNel
      .map(_ foldMap (f => firstSegmentName(f).toSet))
      .toRightDisjunction(unknownPath(dir))
  )

  def filesInDir(dir: ADir)(implicit KVF: KVFile[S]): FS[Vector[AFile]] =
    ls(dir) flatMap {
      case -\/(_)     => Vector()
      case \/-(names) => names.toVector map (dir / _) flatMap (x => maybeFile(x))
    }

  def existsError[A](path: APath): FLR[A]         = point(-\/(pathErr(pathExists(path))))
  def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))
  def nextLong(implicit MS: MonotonicSeq :<: S)   = MonotonicSeq.Ops[S].next

  def deleteDir(x: ADir)(implicit KVF: KVFile[S]): FLR[Unit] = filesInDir(x) flatMap {
    case Seq() => unknownPath(x)
    case fs    => ignoreRes(fs traverse (KVF delete _))
  }
  def deleteFile(x: AFile)(implicit KVF: KVFile[S]): FLR[Unit] = KVF contains x flatMap {
    case false => unknownPath(x)
    case true  => ignoreRes(KVF.delete(x))
  }

  def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics)(implicit KVF: KVFile[S]): FLR[Unit] = filesInDir(src) flatMap {
    case Seq() => unknownPath(src)
    case fs    =>
      def move0 = ignoreRes(fs traverse (f => KVF.move(f, dst / fileName(f))))
      semantics match {
        case Overwrite     => move0
        case FailIfExists  => filesInDir(dst) flatMap (xs => xs.nonEmpty.fold(pathErr(pathExists(dst)), move0))
        case FailIfMissing => filesInDir(dst) flatMap (xs => xs.isEmpty.fold(unknownPath(dst), move0))
      }
  }

  def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics)(implicit KVF: KVFile[S]): FLR[Unit] = {
    KVF contains src flatMap {
      case false => unknownPath(src)
      case _     =>
        KVF contains dst flatMap {
          case true if semantics == FailIfExists   => pathErr(pathExists(dst))
          case false if semantics == FailIfMissing => unknownPath(dst)
          case _                                   => ignoreRes(KVF.move(src, dst))
        }
    }
  }

  def manageFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]) = λ[ManageFile ~> FS] {
    case ManageFile.Move(DirToDir(src, dst), semantics)   => moveDir(src, dst, semantics)
    case ManageFile.Move(FileToFile(src, dst), semantics) =>
      KVF contains src flatMap {
        case false => unknownPath(src)
        case _     =>
          KVF contains dst flatMap {
            case true if semantics == FailIfExists   => pathErr(pathExists(dst))
            case false if semantics == FailIfMissing => unknownPath(dst)
            case _                                   => ignoreRes(KVF.move(src, dst))
          }
      }

    case ManageFile.Delete(path)                          => refineType(path).fold(deleteDir, deleteFile)
    case ManageFile.TempFile(path)                        =>
      for {
        uid <- nextLong
        file = path.parentOrSelf / tmpName(uid)
        _   <- KVF.put(file, emptyData())
      } yield file
  }

  def writeFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]) = λ[WriteFile ~> FS] {
    case WriteFile.Write(fh, chunks) =>
      KVW get fh flatMap {
        case None                      => Vector(unknownWriteHandle(fh))
        case Some(WritePos(file, offset)) =>
          KVF get file flatMap {
            case None       => Vector(unknownPath(file))
            case Some(data) =>
              for {
                _ <- KVF.put(file, (data take offset) ++ chunks ++ (data drop offset))
                _ <- KVW.put(fh, WritePos(file, offset + chunks.length))
              } yield Vector()
          }
      }
    case WriteFile.Open(file)        =>
      for {
        uid <- nextLong
        h = WHandle(file, uid)
        _ <- KVF.put(file, emptyData())
        _ <- KVW.put(h, WritePos(file, 0))
      } yield h

    case WriteFile.Close(fh) => (KVW delete fh).void
  }
  def readFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]) = {
    def openForRead(file: AFile, offset: Int, limit: Option[Int]): FLR[RHandle] = KVF get file flatMap {
      case None       => unknownPath(file)
      case Some(data) =>
        for {
          uid <- nextLong
          h = RHandle(file, uid)
          _ <- KVR.put(h, ReadPos(file, offset, limit getOrElse data.length))
        } yield h
    }

    λ[ReadFile ~> FS] {
      case ReadFile.Open(file, offset, limit) => openForRead(file, offset.get.toInt, limit map (_.get.toInt))
      case ReadFile.Close(fh)                 => (KVR delete fh).void
      case ReadFile.Read(fh)                  =>
        KVR get fh flatMap {
          case None                               => unknownReadHandle(fh)
          case Some(ReadPos(file, offset, limit)) =>
            // NB: no data
            KVF get file flatMap {
              case None       => Vector() // unknownPath(file)
              case Some(data) =>
                val chunks = data drop offset take limit
                val newPos = ReadPos(file, offset + chunks.length, limit - chunks.length)
                for (_ <- KVR.put(fh, newPos)) yield chunks
            }
        }
    }
  }
  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) = λ[QueryFile ~> FS] {
    case QueryFile.ExecutePlan(lp, out) => phaseResults(lp) tuple \/-(out)
    case QueryFile.EvaluatePlan(lp)     => (phaseResults(lp) |@| nextLong)((ph, uid) => ph -> \/-(QHandle(uid)))
    case QueryFile.Explain(lp)          => phaseResults(lp) tuple ExecutionPlan(FsType, "...")
    case QueryFile.ListContents(dir)    => ls(dir)
    case QueryFile.FileExists(file)     => KVF contains file
    case QueryFile.More(qh)             => Vector()
    case QueryFile.Close(fh)            => (KVQ delete fh).void
  }

  def boundFs(implicit
    TS: Task :<: S,
    KVF: KVFile[S],
    KVR: KVRead[S],
    KVW: KVWrite[S],
    KVQ: KVQuery[S],
    MS: MonotonicSeq :<: S
  ): BoundFilesystem[S] = BoundFilesystem(queryFile, readFile, writeFile, manageFile)
}

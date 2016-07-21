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

package quasar.fs.mount

import quasar.Predef._
import quasar._
import quasar.effect._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._, FileSystemError._, PathError._

import matryoshka.{free => _, _}
import pathy.Path._
import scalaz._, Scalaz._

object view {
  /** Translate reads on view paths to the equivalent queries. */
  def readFile[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: QueryFile :<: S,
    S2: MonotonicSeq :<: S,
    S3: ViewState :<: S,
    S4: MountConfigs :<: S
  ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val seq = MonotonicSeq.Ops[S]
    val viewState = ViewState.Ops[S]

    def openFile(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] =
      for {
        rh <- readUnsafe.open(f, off, lim)
        h  <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
        _  <- viewState.put(h, ResultSet.Read(rh)).liftM[FileSystemErrT]
      } yield h

    def openView(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] = {
      val readLP = addOffsetLimit(LogicalPlan.Read(f), off, lim)

      def dataHandle(data: List[Data]): Free[S, ReadHandle] =
        for {
          h <- seq.next.map(ReadHandle(f, _))
          _ <- viewState.put(h, ResultSet.Data(data.toVector))
        } yield h

      def queryHandle(lp: Fix[LogicalPlan]): FileSystemErrT[Free[S, ?], ReadHandle] =
        for {
          qh <- EitherT(queryUnsafe.eval(lp).run.value)
          h  <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
          _  <- viewState.put(h, ResultSet.Results(qh)).liftM[FileSystemErrT]
        } yield h

      for {
        lp <- ViewMounter.rewrite[S](readLP).leftMap(se =>
                planningFailed(readLP, Planner.InternalError(se.shows)))
        h  <- refineConstantPlan(lp).fold(d => dataHandle(d).liftM[FileSystemErrT], queryHandle)
      } yield h
    }

    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]): Free[S, A] = rf match {
        case Open(file, off, lim) =>
          ViewMounter.exists[S](file).ifM(
            openView(file, off, lim).run,
            openFile(file, off, lim).run)

        case Read(handle) =>
          viewState.get(handle).toRight(unknownReadHandle(handle)).flatMap {
            case ResultSet.Data(values) =>
              viewState.put(handle, ResultSet.Data(Vector.empty))
                .as(values)
                .liftM[FileSystemErrT]

            case ResultSet.Read(handle) =>
              readUnsafe.read(handle)

            case ResultSet.Results(handle) =>
              queryUnsafe.more(handle)
          }.run

        case Close(handle) =>
          (viewState.get(handle) <* viewState.delete(handle).liftM[OptionT]).flatMapF {
            case ResultSet.Data(_)         => ().point[Free[S, ?]]
            case ResultSet.Read(handle)    => readUnsafe.close(handle)
            case ResultSet.Results(handle) => queryUnsafe.close(handle)
          }.getOrElse(())
      }
    }
  }

  /** Intercept and fail any write to a view path; all others are passed untouched. */
  def writeFile[S[_]](
    implicit
    S0: WriteFile :<: S,
    S1: MountConfigs :<: S
  ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    val writeUnsafe = WriteFile.Unsafe[S]

    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]): Free[S, A] = wf match {
        case Open(p) =>
          ViewMounter.exists[S](p).ifM(
            pathErr(invalidPath(p, "Cannot write to a view."))
              .left[WriteHandle].point[Free[S, ?]],
            writeUnsafe.open(p).run)

        case Write(h, chunk) =>
          writeUnsafe.write(h, chunk)

        case Close(h) =>
          writeUnsafe.close(h)
      }
    }
  }

  /** Intercept and handle moves and deletes involving view path(s); all others are passed untouched. */
  def manageFile[S[_]](
    implicit
    S0: ManageFile :<: S,
    S1: QueryFile :<: S,
    S2: MountConfigs :<: S
  ): ManageFile ~> Free[S, ?] = {
    import ManageFile._
    import MoveSemantics._

    val manage = ManageFile.Ops[S]
    val query = QueryFile.Ops[S]

    def dirToDirMove(src: ADir, dst: ADir, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] = {
      def move(pathSegments: Set[PathSegment]): FileSystemErrT[Free[S, ?], Unit] =
        pathSegments.toList.traverse_ { s =>
          EitherT(s.fold(
            d => dirToDirMove(src </> dir1(d), dst </> dir1(d), semantics),
            f => fileToFileMove(src </> file1(f), dst </> file1(f), semantics)))
        }

      ViewMounter.ls[S](src).flatMap { ps =>
        if (ps.isEmpty)
          manage.moveDir(src, dst, semantics).run
        else
          // TODO: This runs 'move' even if moveDir fails, is that intended?
          manage.moveDir(src, dst, semantics).run *> move(ps).run
      }
    }

    def fileToFileMove(src: AFile, dst: AFile, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] =
      (ViewMounter.exists[S](src) |@| ViewMounter.exists[S](dst) |@| query.fileExists(dst)).tupled.flatMap {
        case (srcViewExists, dstViewExists, dstFileExists) => (semantics match {
          case FailIfExists if dstViewExists || dstFileExists =>
            pathErr(pathExists(dst)).raiseError[manage.M, Unit]

          case FailIfMissing if !(dstViewExists || dstFileExists) =>
            pathErr(pathNotFound(dst)).raiseError[manage.M, Unit]

          case _ if srcViewExists && dstFileExists =>
            // NB: We ignore the result of the filesystem delete as we're willing to
            //     shadow existing files if it fails for any reason.
            (manage.delete(dst).run *> ViewMounter.move[S](src, dst)).liftM[FileSystemErrT]

          case _ if srcViewExists && !dstFileExists =>
            ViewMounter.move[S](src, dst).liftM[FileSystemErrT]

          case _ =>
            manage.moveFile(src, dst, semantics)
        }).run
    }

    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scenario, semantics) =>
          scenario.fold(
            (src, dst) => dirToDirMove(src, dst, semantics),
            (src, dst) => fileToFileMove(src, dst, semantics))

        case Delete(path) =>
          val delete = manage.delete(path)
          ViewMounter.viewPaths[S].flatMap { viewPaths =>
            refineType(path).fold(
              d => viewPaths.foldMapM(f =>
                     ViewMounter.delete[S](f) whenM f.relativeTo(d).isDefined
                   ).liftM[FileSystemErrT] *> delete,

              f => viewPaths.contains(f).fold(
                     ViewMounter.delete[S](f).liftM[FileSystemErrT],
                     delete)
            ).run
          }

        case TempFile(nearTo) =>
          manage.tempFile(nearTo).run
      }
    }
  }

  /** Intercept and rewrite queries involving views, and overlay views when
    * enumerating files and directories. */
  def queryFile[S[_]](
    implicit
    S0: QueryFile :<: S,
    S1: MountConfigs :<: S
  ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    import query.transforms.ExecM

    def rewrite[A](lp: Fix[LogicalPlan], op: Fix[LogicalPlan] => ExecM[A]) =
      ViewMounter.rewrite[S](lp).run.flatMap(_.fold(
        e => planningFailed(lp, Planner.InternalError(e.shows)).raiseError[ExecM, A],
        p => op(p)).run.run)

    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          rewrite(lp, query.execute(_, out))

        case EvaluatePlan(lp) =>
          rewrite(lp, queryUnsafe.eval)

        case More(handle) =>
          queryUnsafe.more(handle).run

        case Close(handle) =>
          queryUnsafe.close(handle)

        case Explain(lp) =>
          rewrite(lp, query.explain)

        case ListContents(dir) =>
          (ViewMounter.ls[S](dir) |@| query.ls(dir).run)((vls, qls) => qls match {
            case  \/-(ps) =>
              (ps ++ vls).right
            case -\/(err @ PathErr(PathNotFound(_))) =>
              if (vls.nonEmpty) vls.right
              else err.left
            case -\/(v) => v.left
          })

        case FileExists(file) =>
          ViewMounter.exists[S](file).ifM(
            true.point[Free[S, ?]],
            query.fileExists(file))
      }
    }
  }

  /** Translates requests which refer to any view path into operations
    * on an underlying filesystem, where references to views have been
    * rewritten as queries against actual files.
    */
  def fileSystem[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S4: MonotonicSeq :<: S,
    S5: ViewState :<: S,
    S6: MountConfigs :<: S
  ): FileSystem ~> Free[S, ?] =
    interpretFileSystem[Free[S, ?]](queryFile, readFile, writeFile, manageFile)
}

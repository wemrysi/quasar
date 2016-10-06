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
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fs._
import quasar.fp.ski._
import quasar.fp._, free._

import pathy.Path, Path._
import scalaz._, Scalaz._

object Mounter {
  import Mounting._, MountConfig._

  /** `Mounting` interpreter interpreting into `KeyValueStore`, using the
    * supplied functions to handle mount and unmount requests.
    *
    * @param mount   Called to handle a mount request.
    * @param unmount Called to handle the removal/undoing of a mount request.
    */
  def apply[F[_], S[_]](
    mount: MountRequest => F[MountingError \/ Unit],
    unmount: MountRequest => F[Unit]
  )(implicit
    S0: F :<: S,
    S1: MountConfigs :<: S
  ): Mounting ~> Free[S, ?] = {

    type FreeS[A]  = Free[S, A]
    type MntE[A]   = MntErrT[FreeS, A]

    val mountConfigs = KeyValueStore.Ops[APath, MountConfig, S]
    val merr = MonadError[MntE, MountingError]

    def mount0(req: MountRequest): MntE[Unit] =
      EitherT[FreeS, MountingError, Unit](free.lift(mount(req)).into[S])

    def unmount0(req: MountRequest): FreeS[Unit] =
      free.lift(unmount(req)).into[S]

    def failIfExisting(path: APath): MntE[Unit] =
      mountConfigs.get(path).as(pathExists(path)).toLeft(())

    def handleRequest(req: MountRequest): MntE[Unit] = {
      val putOrUnmount: MntE[Unit] =
        mountConfigs.compareAndPut(req.path, None, req.toConfig)
          .liftM[MntErrT].ifM(
            merr.point(()),
            unmount0(req).liftM[MntErrT] <*
              merr.raiseError(pathExists(req.path)))

      failIfExisting(req.path) *> mount0(req) *> putOrUnmount
    }

    def lookupType(p: APath): OptionT[FreeS, MountType] =
      mountConfigs.get(p).map {
        case ViewConfig(_, _)         => MountType.viewMount()
        case FileSystemConfig(tpe, _) => MountType.fileSystemMount(tpe)
      }

    def forPrefix(dir: ADir): FreeS[Set[APath]] =
      mountConfigs.keys.map(_
        .filter(p => (dir: APath) ≠ p && p.relativeTo(dir).isDefined)
        .toSet)

    def handleUnmount(path: APath): OptionT[FreeS, Unit] =
      mountConfigs.get(path)
        .flatMap(cfg => OptionT(mkMountRequest(path, cfg).point[FreeS]))
        .flatMapF(req => mountConfigs.delete(path) *> unmount0(req))

    def handleRemount(src: APath, dst: APath): MntE[Unit] = {
      def reqOrFail(path: APath, cfg: MountConfig): MntE[MountRequest] =
        OptionT(mkMountRequest(path, cfg).point[FreeS])
          .toRight(MountingError.invalidConfig(cfg, "config type mismatch".wrapNel))

      for {
        cfg    <- mountConfigs.get(src).toRight(pathNotFound(src))
        srcReq <- reqOrFail(src, cfg)
        dstReq <- reqOrFail(dst, cfg)
        _      <- mountConfigs.delete(src).liftM[MntErrT]
        _      <- unmount0(srcReq).liftM[MntErrT]
        upd    =  mountConfigs.compareAndPut(dst, None, dstReq.toConfig)
        _      <- OptionT(upd.map(_.option(()))).toRight(pathExists(dst))
        _      <- mount0(dstReq)
      } yield ()
    }

    λ[Mounting ~> FreeS] {
      case HavingPrefix(dir) =>
        for {
          paths <- forPrefix(dir)
          pairs <- paths.toList.traverse(p => lookupType(p).strengthL(p).run)
        } yield pairs.flatMap(_.toList).toMap

      case LookupType(path) =>
        lookupType(path).run

      case LookupConfig(path) =>
        mountConfigs.get(path).run

      case MountView(loc, query, vars) =>
        handleRequest(MountRequest.mountView(loc, query, vars)).run

      case MountFileSystem(loc, typ, uri) =>
        handleRequest(MountRequest.mountFileSystem(loc, typ, uri)).run

      case Unmount(path) =>
        handleUnmount(path)
          .flatMapF(_ =>
            refineType(path).swap.foldMap(forPrefix)
              .flatMap(_.toList.traverse_(handleUnmount(_)).run.void))
          .toRight(pathNotFound(path))
          .run

      case Remount(src, dst) =>
        if (src ≟ dst)
          mountConfigs.get(src).void.toRight(pathNotFound(src)).run
        else {
          def move[T](srcDir: ADir, dstDir: ADir, p: Path[Abs,T,Sandboxed]): FreeS[Unit] =
            p.relativeTo(srcDir)
              .map(rel => handleRemount(p, dstDir </> rel).run.void)
              .sequence_

          val moveNested: FreeS[Unit] =
            (maybeDir(src) |@| maybeDir(dst))((srcDir, dstDir) =>
              forPrefix(srcDir).flatMap(_.toList.traverse_(move(srcDir, dstDir, _)))).sequence_

          failIfExisting(dst) *>
            handleRemount(src, dst) <*
            moveNested.liftM[MntErrT]
        }.run
    }
  }

  /** A mounter where all mount requests succeed trivially.
    *
    * Useful in scenarios where only the bookkeeping of mounts is needed.
    */
  def trivial[S[_]](implicit S: MountConfigs :<: S) = {
    type F[A] = Coproduct[Id, S, A]
    val mnt   = Mounter[Id, F](κ(().right), κ(()))

    λ[Mounting ~> Free[S, ?]](m => mnt(m) foldMap (pointNT[Free[S, ?]] :+: liftFT[S]))
  }

  private val pathNotFound = MountingError.pathError composePrism PathError.pathNotFound
  private val pathExists   = MountingError.pathError composePrism PathError.pathExists

  private def mkMountRequest(path: APath, cfg: MountConfig): Option[MountRequest] =
    refineType(path).fold(
      d => fileSystemConfig.getOption(cfg) map { case (t, u) =>
        MountRequest.mountFileSystem(d, t, u)
      },
      f => viewConfig.getOption(cfg) map { case (q, vs) =>
        MountRequest.mountView(f, q, vs)
      })
}

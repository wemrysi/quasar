/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil.vfs

import quasar.contrib.pathy.{ADir, AFile, APath}

import fs2.{Stream, Sink}

import scalaz.Free

import scodec.bits.ByteVector

import java.util.UUID

sealed trait POSIXOp[A] extends Product with Serializable

object POSIXOp {

  case object GenUUID extends POSIXOp[UUID]

  def genUUID: POSIX[UUID] = Free.liftF(GenUUID)

  final case class OpenW(target: AFile) extends POSIXOp[Sink[POSIXWithTask, ByteVector]]

  def openW(target: AFile): POSIX[Sink[POSIXWithTask, ByteVector]] =
    Free.liftF(OpenW(target))

  final case class OpenR(target: AFile) extends POSIXOp[Stream[POSIXWithTask, ByteVector]]

  def openR(target: AFile): POSIX[Stream[POSIXWithTask, ByteVector]] =
    Free.liftF(OpenR(target))

  final case class MkDir(target: ADir) extends POSIXOp[Unit]

  def mkDir(target: ADir): POSIX[Unit] = Free.liftF(MkDir(target))

  final case class LinkDir(src: ADir, target: ADir) extends POSIXOp[Unit]

  def linkDir(src: ADir, target: ADir): POSIX[Unit] =
    Free.liftF(LinkDir(src, target))

  final case class LinkFile(src: AFile, target: AFile) extends POSIXOp[Unit]

  def linkFile(src: AFile, target: AFile): POSIX[Unit] =
    Free.liftF(LinkFile(src, target))

  final case class Move(src: AFile, target: AFile) extends POSIXOp[Unit]

  def move(src: AFile, target: AFile): POSIX[Unit] =
    Free.liftF(Move(src, target))

  final case class Exists(target: APath) extends POSIXOp[Boolean]

  def exists(target: APath): POSIX[Boolean] = Free.liftF(Exists(target))

  final case class Delete(target: APath) extends POSIXOp[Unit]

  def delete(target: APath): POSIX[Unit] = Free.liftF(Delete(target))
}

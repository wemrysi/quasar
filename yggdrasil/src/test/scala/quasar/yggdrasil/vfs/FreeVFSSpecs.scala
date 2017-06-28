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

import quasar.contrib.pathy.{ADir, AFile, RPath}

import fs2.{Stream, Sink}

import org.specs2.mutable._

import pathy.Path

import scalaz.Coproduct
import scalaz.concurrent.Task

import scodec.bits.ByteVector

import smock._

import java.util.UUID

object FreeVFSSpecs extends Specification {
  import POSIXOp._
  import StreamTestUtils._

  type S[A] = Coproduct[POSIXOp, Task, A]

  val H = Harness[S, Task]

  val BaseDir = Path.rootDir </> Path.dir("foo")

  "vfs layer" should {

    "initialize from an empty state" in {
      val interp = for {
        _ <- H.pattern[Boolean] {
          case CPL(Exists(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.dir("META"))

              false
            }
        }

        _ <- H.pattern[Unit] {
          case CPL(MkDir(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.dir("META"))
            }
        }

        _ <- vlogInit(BaseDir </> Path.dir("META"), None)

        _ <- persistMeta(
          BaseDir </> Path.dir("META"),
          (Map(), _ mustEqual "{}"),
          (Map(), _ mustEqual "{}"))

        _ <- H.pattern[List[RPath]] {
          case CPL(Ls(target)) =>
            Task delay {
              target mustEqual BaseDir

              List(Path.dir("META"))
            }
        }
      } yield ()

      val vfs = interp(FreeVFS.init[S](BaseDir)).unsafePerformSync

      vfs must beLike {
        case VFS(BaseDir, VersionLog(vlogBase, committed, versions), paths, index, vlogs, blobs) =>
          paths must beEmpty
          index must beEmpty
          vlogs must beEmpty
          blobs must beEmpty

          vlogBase mustEqual (BaseDir </> Path.dir("META"))
          committed must haveSize(1)
          versions mustEqual committed.toSet
      }
    }
  }

  def vlogInit(baseDir: ADir, versions: Option[List[Version]]): Harness[S, Task, Unit] = {
    for {
      _ <- H.pattern[Boolean] {
        case CPL(Exists(target)) =>
          Task delay {
            target mustEqual (baseDir </> Path.file("versions.json"))

            versions.isDefined
          }
      }

      _ <- versions match {
        case Some(versions) =>
          for {
            _ <- H.pattern[Stream[POSIXWithTask, ByteVector]] {
              case CPL(OpenR(target)) =>
                Task delay {
                  target mustEqual (baseDir </> Path.file("versions.json"))

                  Stream(ByteVector(versions.map(_.value.toString).mkString("[", ",", "]").getBytes))
                }
            }

            _ <- H.pattern[List[RPath]] {
              case CPL(Ls(target)) =>
                Task delay {
                  target mustEqual baseDir

                  versions.map(_.value.toString).map(Path.dir(_))
                }
            }
          } yield ()

        case None =>
          for {
            _ <- H.pattern[Sink[POSIXWithTask, ByteVector]] {
              case CPL(OpenW(target)) =>
                Task delay {
                  target mustEqual (baseDir </> Path.file("versions.json.new"))

                  (_ => Stream.empty)
                }
            }

            _ <- H.pattern[Unit] {
              case CPL(Move(from, to)) =>
                Task delay {
                  from mustEqual (baseDir </> Path.file("versions.json.new"))
                  to mustEqual (baseDir </> Path.file("versions.json"))
                }
            }
          } yield ()
      }

      _ <- H.pattern[List[RPath]] {
        case CPL(Ls(target)) =>
          Task delay {
            target mustEqual baseDir

            versions.toList.flatMap(_.map(v => Path.dir(v.value.toString)))
          }
      }
    } yield ()
  }

  def persistMeta(
      baseDir: ADir,
      pathPair: (Map[AFile, Blob], String => Unit),
      indexPair: (Map[ADir, Vector[RPath]], String => Unit)): Harness[S, Task, Unit] = {

    val (paths, pathTest) = pathPair
    val (index, indexTest) = indexPair

    for {
      uuid <- H.patternWithState[UUID, UUID] {
        case CPL(GenUUID) =>
          Task delay {
            val uuid = UUID.randomUUID()

            (uuid, uuid)
          }
      }

      _ <- H.pattern[Unit] {
        case CPL(MkDir(target)) =>
          Task delay {
            target mustEqual (baseDir </> Path.dir(uuid.toString))
          }
      }

      _ <- H.pattern[Sink[POSIXWithTask, ByteVector]] {
        case CPL(OpenW(target)) =>
          Task delay {
            target mustEqual (baseDir </> Path.dir(uuid.toString) </> Path.file("paths.json"))
            assertionSink(pathTest)
          }
      }

      _ <- H.pattern[Unit] {
        case CPR(ta) => ta
      }

      _ <- H.pattern[Sink[POSIXWithTask, ByteVector]] {
        case CPL(OpenW(target)) =>
          Task delay {
            target mustEqual (baseDir </> Path.dir(uuid.toString) </> Path.file("index.json"))
            assertionSink(indexTest)
          }
      }

      _ <- H.pattern[Unit] {
        case CPR(ta) => ta
      }

      _ <- H.pattern[Sink[POSIXWithTask, ByteVector]] {
        case CPL(OpenW(target)) =>
          Task delay {
            target mustEqual (baseDir </> Path.file("versions.json.new"))
            assertionSink(_ mustEqual s"""["${uuid.toString}"]""")
          }
      }

      _ <- H.pattern[Unit] {
        case CPR(ta) => ta
      }

      _ <- H.pattern[Unit] {
        case CPL(Move(from, to)) =>
          Task delay {
            from mustEqual (baseDir </> Path.file("versions.json.new"))
            to mustEqual (baseDir </> Path.file("versions.json"))
          }
      }
    } yield ()
  }

  object CPR {
    def unapply[A](cp: Coproduct[POSIXOp, Task, A]): Option[Task[A]] =
      cp.run.toOption
  }

  object CPL {
    def unapply[A](cp: Coproduct[POSIXOp, Task, A]): Option[POSIXOp[A]] =
      cp.run.swap.toOption
  }
}

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

import quasar.contrib.pathy.APath

import fs2.{Sink, Stream}
import fs2.interop.scalaz._

import org.specs2.mutable._

import pathy.Path

import scalaz.Coproduct
import scalaz.concurrent.Task

import scodec.bits.ByteVector

import smock._

import java.util.UUID

object VersionLogSpecs extends Specification {
  import POSIXOp._

  "version log manager" should {
    val H = Harness[POSIXOp, Task]
    val HWT = Harness[Coproduct[POSIXOp, Task, ?], Task]

    val BaseDir = Path.rootDir </> Path.dir("foo")

    "read and return empty log" in {
      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              true
            }
        }

        _ <- HWT.pattern[Stream[POSIXWithTask, ByteVector]] {
          case CPL(OpenR(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              Stream(ByteVector("[]".getBytes))
            }
        }

        _ <- HWT.pattern[List[APath]] {
          case CPL(Ls(BaseDir)) => Task.now(Nil)
        }
      } yield ()

      val result =
        interp(VersionLog.init[Coproduct[POSIXOp, Task, ?]](BaseDir)).unsafePerformSync

      result mustEqual VersionLog(BaseDir, Nil, Set())
    }

    "return and init empty log when does not exist" in {
      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              false
            }
        }

        _ <- HWT.pattern[Sink[POSIXWithTask, ByteVector]] {
          case CPL(OpenW(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json.new"))

              val sink: Sink[POSIXWithTask, ByteVector] = { s =>
                s flatMap { bv =>
                  Stream suspend {
                    new String(bv.toArray) mustEqual "[]"

                    Stream.empty
                  }
                }
              }

              sink
            }
        }

        _ <- HWT.pattern[Unit] {
          case CPL(Move(from, to)) =>
            Task delay {
              from mustEqual (BaseDir </> Path.file("versions.json.new"))
              to mustEqual (BaseDir </> Path.file("versions.json"))
            }
        }

        _ <- HWT.pattern[List[APath]] {
          case CPL(Ls(BaseDir)) => Task.now(Nil)
        }
      } yield ()

      val result =
        interp(VersionLog.init[Coproduct[POSIXOp, Task, ?]](BaseDir)).unsafePerformSync

      result mustEqual VersionLog(BaseDir, Nil, Set())
    }

    "read and return empty log with uncommitted versions" in {
      val versions = (UUID.randomUUID() :: UUID.randomUUID() :: Nil).map(Version)

      val members =
        Path.file("versions.json") :: versions.map(v => Path.dir(v.value.toString))

      val vpaths = members.map(BaseDir </> _)

      val interp = for {
        _ <- HWT.pattern[Boolean] {
          case CPL(Exists(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              true
            }
        }
        _ <- HWT.pattern[Stream[POSIXWithTask, ByteVector]] {
          case CPL(OpenR(target)) =>
            Task delay {
              target mustEqual (BaseDir </> Path.file("versions.json"))

              Stream(ByteVector("[]".getBytes))
            }
        }

        _ <- HWT.pattern[List[APath]] {
          case CPL(Ls(BaseDir)) => Task.now(vpaths)
        }
      } yield ()

      val result =
        interp(VersionLog.init[Coproduct[POSIXOp, Task, ?]](BaseDir)).unsafePerformSync

      result mustEqual VersionLog(BaseDir, Nil, versions.toSet)
    }
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

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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.fs._
import quasar.fs.mount.FileSystemDef, FileSystemDef.DefErrT
import quasar.physical.sparkcore.fs.{readfile => corereadfile}
import quasar.effect._
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs.mount.ConnectionUri

import java.io.PrintWriter

import org.apache.spark._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object local {
  val FsType = FileSystemType("sparklocal")

  type Eff0[A] = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], Read[SparkContext, ?], A]
  type Eff1[A] = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Eff0, A]
  type Eff2[A] = Coproduct[Task, Eff1, A]
  type Eff[A] = Coproduct[MonotonicSeq, Eff2, A]

  final case class SparkFSDef[S[_]](run: Free[Eff, ?] ~> Free[S, ?], close: Free[S, Unit])

  def sparkFsDef[S[_]](uri: ConnectionUri)(implicit
    S0: Task :<: S
  ): Free[S, SparkFSDef[S]] = {

    val genSc = Task.delay {
      val conf = new SparkConf().setMaster(uri.value).setAppName ("quasar")
      new SparkContext(conf)
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, PrintWriter]) |@|
      genSc) {
      (genState, sparkCursors, printWriters, sc) =>
        val taskInter: Eff ~> Task = MonotonicSeq.fromTaskRef(genState) :+:
          NaturalTransformation.refl[Task] :+:
          KeyValueStore.fromTaskRef[WriteHandle, PrintWriter](printWriters) :+:
          KeyValueStore.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) :+:
            Read.constant[Task, SparkContext](sc)

        val sInter: Eff ~> S = taskInter andThen injectNT[Task, S]

      SparkFSDef(mapSNT[Eff, S](sInter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S):
      FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        val fsDef = sparkFsDef(uri)

        fsDef.map { case SparkFSDef(run, close) =>
          FileSystemDef.DefinitionResult[Free[S, ?]]({

          val interpreter: FileSystem ~> Free[Eff, ?] = interpretFileSystem(
            Empty.queryFile[Free[Eff, ?]],
            corereadfile.interpret(readfile.input[Eff]),
            writefile.interpret[Eff],
            managefile.interpret[Eff])

            interpreter andThen run

          },
          close)
      }.liftM[DefErrT]
    }
}

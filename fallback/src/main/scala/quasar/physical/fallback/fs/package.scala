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

package quasar.physical.fallback

import quasar.Predef._
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp.TaskRef
import quasar.fp.free._
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef._
import java.io.PrintWriter
import java.nio.file.{ Files, Paths }
import scala.collection.JavaConverters._
import quasar.physical.fallback.fs.corequeryfile.RddState
import pathy.Path.posixCodec

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import java.time._
import scalaz.syntax.OrderOps
import java.lang.Comparable

package fs {
  final case class SparkContext(conf: SparkConf) {
    def stop(): Unit                              = ()
    def textFile(path: String): RDD[String]       = RDD((Files readAllLines (Paths get path)).asScala.toVector)
    def parallelize[A: CTag](seq: Seq[A]): RDD[A] = RDD[A](seq.toVector)
  }
  final case class SparkConf(master: String = "<undef>", app: String = "<undef>") {
    def setMaster(master: String): SparkConf = SparkConf(master, app)
    def setAppName(app: String): SparkConf   = SparkConf(master, app)
  }
}

package object fs {
  val FsType = FileSystemType("fallback")

  def TODO: Nothing                    = scala.Predef.???
  def instantMillis(x: Instant): Long  = x.getEpochSecond * 1000
  def nowMillis(): Long                = instantMillis(Instant.now)
  def instantFromMillis(millis: Long)  = Instant ofEpochMilli millis
  def zonedUtcFromMillis(millis: Long) = ZonedDateTime.ofInstant(instantFromMillis(millis), ZoneOffset.UTC)
  def UTC                              = ZoneOffset.UTC

  implicit def comparableOrder[A, B >: A <: Comparable[B]](x: A with Comparable[B]): Order[B] =
    Order order ((x, y) => Ordering fromInt (x compareTo y))

  implicit def comparableOrderOps[A, B >: A <: Comparable[B]](x: A with Comparable[B]): OrderOps[B] =
    ToOrderOps[B](x)(comparableOrder[A, B](x))

  type CTag[A] = scala.reflect.ClassTag[A]
  type Data    = quasar.Data
  val Data     = quasar.Data

  type EffM1[A] = Coproduct[KeyValueStore[ResultHandle, RddState, ?], Read[SparkContext, ?], A]
  type Eff0[A]  = Coproduct[KeyValueStore[ReadHandle, SparkCursor, ?], EffM1, A]
  type Eff1[A]  = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Eff0, A]
  type Eff2[A]  = Coproduct[Task, Eff1, A]
  type Eff3[A]  = Coproduct[PhysErr, Eff2, A]
  type Eff[A]   = Coproduct[MonotonicSeq, Eff3, A]

  final case class SparkFSConf(sparkConf: SparkConf, prefix: ADir)

  def parseUri(uri: ConnectionUri): DefinitionError \/ SparkFSConf = {
    def tmpDir() = Files.createTempDirectory("fallback")
    def error(msg: String): DefinitionError \/ SparkFSConf =
      NonEmptyList(msg).left[EnvironmentError].left[SparkFSConf]

    val dir  = uri.value match {
      case "" => tmpDir()
      case _  => (Paths get uri.value).toAbsolutePath
    }
    val dir1 = if (dir.toFile.isDirectory) dir else tmpDir()
    println(s"Working directory is $dir1")

    posixCodec parseAbsDir (dir1.toString + "/") match {
      case Some(p) => SparkFSConf(SparkConf("master"), sandboxAbs(p)).right
      case _       => error(s"Couldn't find a usable filesystem path based on $dir1")
    }
  }

  final case class SparkFSDef[S[_]](run: Free[Eff, ?] ~> Free[S, ?], close: Free[S, Unit])

  implicit class IntOps(x: Int) {
    // Like div but to land in the range [1,Limit] instead of [0,Limit).
    def /+(y: Int): Int = ((x - 1) / y) + 1
  }

  private def sparkFsDef[S[_]](sparkConf: SparkConf)(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Free[S, SparkFSDef[S]] = {

    val genSc = Task.delay {
      new SparkContext(sparkConf.setAppName("quasar"))
    }

    lift((TaskRef(0L) |@|
      TaskRef(Map.empty[ResultHandle, RddState]) |@|
      TaskRef(Map.empty[ReadHandle, SparkCursor]) |@|
      TaskRef(Map.empty[WriteHandle, PrintWriter]) |@|
      genSc) {
      // TODO better names!
      (genState, rddStates, sparkCursors, printWriters, sc) =>
      val interpreter: Eff ~> S = (MonotonicSeq.fromTaskRef(genState) andThen injectNT[Task, S]) :+:
      injectNT[PhysErr, S] :+:
      injectNT[Task, S]  :+:
      (KeyValueStore.impl.fromTaskRef[WriteHandle, PrintWriter](printWriters) andThen injectNT[Task, S])  :+:
      (KeyValueStore.impl.fromTaskRef[ReadHandle, SparkCursor](sparkCursors) andThen injectNT[Task, S]) :+:
      (KeyValueStore.impl.fromTaskRef[ResultHandle, RddState](rddStates) andThen injectNT[Task, S]) :+:
      (Read.constant[Task, SparkContext](sc) andThen injectNT[Task, S])

      SparkFSDef(mapSNT[Eff, S](interpreter), lift(Task.delay(sc.stop())).into[S])
    }).into[S]
  }

  private def fsInterpret(fsConf: SparkFSConf): FileSystem ~> Free[Eff, ?] = interpretFileSystem(
    corequeryfile.chrooted[Eff](queryfile.input, FsType, fsConf.prefix),
    corereadfile.chrooted(readfile.input[Eff], fsConf.prefix),
    writefile.chrooted[Eff](fsConf.prefix),
    managefile.chrooted[Eff](fsConf.prefix))

  def definition[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S):
      FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        for {
          fsConf <- EitherT(parseUri(uri).point[Free[S, ?]])
          res <- {
            sparkFsDef(fsConf.sparkConf).map {
              case SparkFSDef(run, close) =>
                FileSystemDef.DefinitionResult[Free[S, ?]](
                  fsInterpret(fsConf) andThen run,
                  close)
            }.liftM[DefErrT]
          }
        }  yield res
    }
}

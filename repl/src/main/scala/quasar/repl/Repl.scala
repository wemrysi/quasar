/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar
package repl

import slamdata.Predef._
import quasar.build.BuildInfo
import quasar.common.{PhaseResultListen, PhaseResultTell}
import quasar.concurrent.BlockingContext
import quasar.mimir.MimirRepr
import quasar.mimir.evaluate.PushdownControl
import quasar.mimir.storage.PTableSchema
import quasar.run.{MonadQuasarErr, Quasar}

import java.io.File

import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.{applicative, flatMap, functor}, applicative._, flatMap._, functor._
import fs2.{io, text, Stream}
import org.apache.commons.io.FileUtils
import org.jline.reader._
import org.jline.terminal._
import scalaz._, Scalaz._

final class Repl[F[_]: ConcurrentEffect: PhaseResultListen](
    prompt: String,
    reader: LineReader,
    evaluator: Command => F[Evaluator.Result[Stream[F, String]]],
    blockingPool: BlockingContext)(
    implicit cs: ContextShift[F]) {

  val F = ConcurrentEffect[F]

  private val read: F[Command] =
    cs.evalOn(Tag.unwrap(blockingPool))(
      F.delay(reader.readLine(prompt))).map(Command.parse)

  private def print(strings: Stream[F, String], blockingPool: BlockingContext): F[Unit] =
    strings
      .map(_ + "\n")
      .through(text.utf8Encode)
      .to(io.stdout[F](Tag.unwrap(blockingPool)))
      .compile.drain

  val loop: F[ExitCode] =
    for {
      cmd <- read
      Evaluator.Result(exitCode, strings) <- evaluator(cmd)
      _ <- print(strings, blockingPool)
      next <- exitCode.fold(loop)(_.pure[F])
    } yield next
}

object Repl {
  def apply[F[_]: ConcurrentEffect: PhaseResultListen: ContextShift](
      prompt: String,
      reader: LineReader,
      evaluator: Command => F[Evaluator.Result[Stream[F, String]]],
      blockingPool: BlockingContext)
      : Repl[F] =
    new Repl[F](prompt, reader, evaluator, blockingPool)

  def mk[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultListen: PhaseResultTell: Timer](
      ref: Ref[F, ReplState],
      pushdown: PushdownControl[F],
      quasar: Quasar[F, MimirRepr, PTableSchema],
      blockingPool: BlockingContext)
      : F[Repl[F]] = {
    val evaluator = Evaluator[F](ref, pushdown, quasar, blockingPool)
    historyFile[F].map(f => Repl[F](prompt, mkLineReader(f), evaluator.evaluate, blockingPool))
  }

  ////

  private val prompt = s"(v${BuildInfo.version}) 💪 $$ "

  private def touch[F[_]](f: File)(implicit F: Sync[F]): F[Option[File]] =
    F.delay {
      \/.fromTryCatchNonFatal(FileUtils.touch(f)) match {
        case -\/(err) => none
        case \/-(_) => f.some
      }
    }

  private def historyFile[F[_]](implicit F: Sync[F]): F[Option[File]] =
    Paths.getProp("quasar.historyfile") >>=
      (_ match {
        case Some(p) => touch(new File(p))
        case None =>
          Paths.getUserHome >>=
            (_ match {
              case Some(h) => touch(new File(h.toFile, ".quasar.history"))
              case None => none[File].pure[F]
            })
      })

  private def ifHistoryFile(builder: LineReaderBuilder, historyFile: Option[File])
      : LineReaderBuilder =
    historyFile.map(
      builder.variable(LineReader.HISTORY_FILE, _)).getOrElse(builder)

  private def mkLineReader(historyFile: Option[File]): LineReader = {
    val terminal = TerminalBuilder.terminal()
    ifHistoryFile(
      LineReaderBuilder.builder().terminal(terminal),
      historyFile).build()
  }

}

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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.catchable._
import quasar.effect.Capture
import quasar.fp._
import quasar.physical.marklogic.xquery._

import scala.collection.JavaConverters._

import com.marklogic.xcc.{Version => _, _}
import com.marklogic.xcc.exceptions._
import com.marklogic.xcc.types.XdmItem
import scalaz._, Scalaz.{ToIdOps => _, _}
import scalaz.stream.Process
import simulacrum.typeclass

@typeclass
trait Xcc[F[_]] extends MonadError_[F, XccError] {
  def currentServerPointInTime: F[BigInt]
  def evaluate(main: MainModule): Process[F, XdmItem]
  def execute(main: MainModule): F[Executed]
  def insert[C[_]: Foldable](content: C[Content]): F[Vector[XccError]]
  def results(main: MainModule): F[Vector[XdmItem]]
  def transact[A](fa: F[A]): F[A]

  def evaluateQuery(query: XQuery): Process[F, XdmItem] =
    evaluate(defaultModule(query))

  def executeQuery(query: XQuery): F[Executed] =
    execute(defaultModule(query))

  def queryResults(query: XQuery): F[Vector[XdmItem]] =
    results(defaultModule(query))

  ////

  private def defaultModule(query: XQuery): MainModule =
    MainModule(Version.`1.0-ml`, ISet.empty, query)
}

object Xcc extends XccInstances

sealed abstract class XccInstances extends XccInstances0 {
  implicit def defaultXcc[F[_]: Monad: Capture: Catchable: SessionReader: CSourceReader]: Xcc[F] =
    new DefaultImpl[F]
}

sealed abstract class XccInstances0 {
  implicit def eitherTXcc[F[_]: Monad: Xcc, E]: Xcc[EitherT[F, E, ?]] =
    new TransXcc[F, EitherT[?[_], E, ?]] {
      def transact[A](fa: EitherT[F, E, A]) =
        EitherT(Xcc[F].transact(fa.run))

      def handleError[A](fa: EitherT[F, E, A])(f: XccError => EitherT[F, E, A]) =
        EitherT(Xcc[F].handleError(fa.run)(f andThen (_.run)))
    }

  implicit def kleisliXcc[F[_]: Monad: Xcc, R]: Xcc[Kleisli[F, R, ?]] =
    new TransXcc[F, Kleisli[?[_], R, ?]] {
      def transact[A](fa: Kleisli[F, R, A]) =
        Kleisli(r => Xcc[F].transact(fa.run(r)))

      def handleError[A](fa: Kleisli[F, R, A])(f: XccError => Kleisli[F, R, A]) =
        Kleisli(r => Xcc[F].handleError(fa.run(r))(f andThen (_.run(r))))
    }

  implicit def stateTXcc[F[_]: Monad: Xcc, S]: Xcc[StateT[F, S, ?]] =
    new TransXcc[F, StateT[?[_], S, ?]] {
      def transact[A](fa: StateT[F, S, A]) =
        StateT(s => Xcc[F].transact(fa.run(s)))

      def handleError[A](fa: StateT[F, S, A])(f: XccError => StateT[F, S, A]) =
        StateT(s => Xcc[F].handleError(fa.run(s))(f andThen (_.run(s))))
    }

  implicit def writerTXcc[F[_]: Monad: Xcc, W: Monoid]: Xcc[WriterT[F, W, ?]] =
    new TransXcc[F, WriterT[?[_], W, ?]] {
      def transact[A](fa: WriterT[F, W, A]) =
        WriterT(Xcc[F].transact(fa.run))

      def handleError[A](fa: WriterT[F, W, A])(f: XccError => WriterT[F, W, A]) =
        WriterT(Xcc[F].handleError(fa.run)(f andThen (_.run)))
    }
}

private[xcc] final class DefaultImpl[F[_]: Monad: Capture: Catchable: SessionReader: CSourceReader] extends Xcc[F] {
  import DefaultImpl.XccXQueryException, Executed.executed, Session.TransactionMode

  def raiseError[A](e: XccError): F[A] =
    e match {
      case XccError.RequestError(c)   => Catchable[F].fail(c)
      case XccError.XQueryError(m, c) => Catchable[F].fail(XccXQueryException(m, c))
    }

  def handleError[A](fa: F[A])(f: XccError => F[A]): F[A] =
    fa handleWith {
      case ex: RequestException         => f(XccError.requestError(ex))
      case     XccXQueryException(m, c) => f(XccError.xqueryError(m, c))
    }

  def currentServerPointInTime: F[BigInt] =
    withSession(_.getCurrentServerPointInTime) map (BigInt(_))

  def evaluate(main: MainModule): Process[F, XdmItem] = {
    def nextItem(rs: ResultSequence): F[Option[ResultItem]] =
      Capture[F].capture(if (rs.hasNext) Some(rs.next) else None)

    def loadItem(ritem: ResultItem): F[XdmItem] =
      Capture[F] capture {
        ritem.cache()
        ritem.getItem
      }

    def next(rs: ResultSequence): F[Option[(XdmItem, ResultSequence)]] =
      (nextItem(rs) >>= (_ traverse loadItem)) map (_ strengthR rs)

    def sessionResults: F[(Session, ResultSequence)] =
      contentsource.defaultSession[F] flatMap { s =>
        SessionReader[F].scope(s)(evaluate0(main, streamingOptions))
          .strengthL(s)
      }

    Process.bracket(sessionResults)(
      srs => Process.eval_(Capture[F].capture(srs._1.close())))(
      srs => Process.unfoldEval(srs._2)(next))
  }

  def execute(main: MainModule): F[Executed] =
    evaluate0(main, streamingOptions)
      .flatMap(rs => Capture[F].capture(rs.close()))
      .as(executed)

  def insert[C[_]: Foldable](content: C[Content]): F[Vector[XccError]] =
    withSession(_.insertContentCollectErrors(content.to[Array]))
      .map(errs => Option(errs).toVector flatMap (_.asScala.toVector))
      .handle { case rex: RequestException => Vector(rex) }
      .map(_ map (XccError.requestError(_)))

  def results(main: MainModule): F[Vector[XdmItem]] =
    evaluate0(main, (new RequestOptions) <| (_.setCacheResult(true)))
      .flatMap(rs => Capture[F] capture {
        val items = rs.toArray.to[Vector]
        rs.close()
        items
      })

  def transact[A](fa: F[A]): F[A] = {
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def completeTxn(restoreTo: TransactionMode)(res: Option[Throwable]): F[Unit] = {
      val txnComplete = restoreTo != TransactionMode.UPDATE
      val restoreMode = setTransactionMode(restoreTo)

      res.cata(
        t => rollback *> restoreMode *> Catchable[F].fail(t),
        txnComplete whenM (commit *> restoreMode))
    }

    beginTransaction >>= (prevMode => fa.ensuring(completeTxn(prevMode)))
  }

  ////

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def beginTransaction: F[TransactionMode] =
    transactionMode >>= { mode =>
      if (mode == TransactionMode.UPDATE)
        mode.point[F]
      else
        setTransactionMode(TransactionMode.UPDATE) as mode
    }

  private def commit: F[Executed] =
    withSession(_.commit).as(executed)

  private def evaluate0(main: MainModule, options: RequestOptions): F[ResultSequence] =
    withSession { s =>
      s.submitRequest(s.newAdhocQuery(main.render, options))
    } handleWith {
      case xex: XQueryException          => Catchable[F].fail(XccXQueryException(main, xex.right))
      case rex: RetryableXQueryException => Catchable[F].fail(XccXQueryException(main, rex.left))
    }

  private def rollback: F[Executed] =
    withSession(_.rollback).as(executed)

  private def setTransactionMode(mode: TransactionMode): F[Executed] =
    withSession(_.setTransactionMode(mode)).as(executed)

  private def streamingOptions: RequestOptions =
    (new RequestOptions) <| (_.setCacheResult(false))

  private def transactionMode: F[TransactionMode] =
    withSession(_.getTransactionMode)

  private def withSession[A](f: Session => A): F[A] =
    SessionReader[F].ask >>= (s => Capture[F].capture(f(s)))
}

private[xcc] object DefaultImpl {
  final case class XccXQueryException(module: MainModule, cause: RetryableXQueryException \/ XQueryException)
    extends Exception(XccError.widenXQueryCause(cause)) {

    def toXccError: XccError =
      XccError.xqueryError(module, cause)

    override def getMessage =
      toXccError.shows
  }
}

private[xcc] sealed abstract class TransXcc[F[_]: Monad: Xcc, T[_[_], _]: MonadTrans] extends Xcc[T[F, ?]] {
  def currentServerPointInTime =
    Xcc[F].currentServerPointInTime.liftM[T]

  def evaluate(main: MainModule) =
    Xcc[F].evaluate(main).translate(liftMT[F, T])

  def execute(main: MainModule) =
    Xcc[F].execute(main).liftM[T]

  def insert[C[_]: Foldable](content: C[Content]) =
    Xcc[F].insert(content).liftM[T]

  def results(main: MainModule) =
    Xcc[F].results(main).liftM[T]

  def raiseError[A](err: XccError) =
    Xcc[F].raiseError(err).liftM[T]
}

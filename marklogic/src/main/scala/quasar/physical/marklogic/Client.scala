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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.effect.Read
import quasar.fp.free._
import quasar.fs._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import com.marklogic.xcc._
import com.marklogic.xcc.exceptions.XQueryException
import com.marklogic.xcc.types._

import scala.collection.JavaConverters._
import scala.math.{ceil, log}

import com.fasterxml.uuid.impl.TimeBasedGenerator
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.stream.io

sealed trait Error
final case class AlreadyExists(path: APath) extends Error
final case class ResourceNotFound(message: String) extends Error
final case class Forbidden(message: String) extends Error
final case class FailedRequest(message: String) extends Error

final case class Client(
  contentSource: ContentSource,
  uuidGenerator: TimeBasedGenerator
) {

  val newSession: Task[Session] = Task.delay(contentSource.newSession)
  def closeSession(s: Session): Task[Unit] = Task.delay(s.close)

  def doInSession[A](f: Session => Task[A]): Task[A] = for {
    session <- newSession
    result  <- f(session)
    _       <- closeSession(session)
  } yield result

  def readDirectory(dir: ADir): Process[Task, ResultItem] = {
    val qopts = {
      val opts = new RequestOptions
      opts.setCacheResult(false)
      opts
    }

    val uri = posixCodec.printPath(dir)

    val xqy = cts.search(
      fn.doc(),
      cts.directoryQuery(uri.xs),
      IList(cts.indexOrder(cts.uriReference, "ascending".xs)))

    io.iteratorR(newSession)(closeSession) { session =>
      val request = session.newAdhocQuery(xqy, qopts)
      Task.delay(session.submitRequest(request).iterator.asScala)
    }
  }

  def moveDocuments_(src: ADir, dst: ADir): Task[Unit] = {
    if (src === dst) Task.now(())
    else {
      val srcUri = posixCodec.printPath(src)
      val dstUri = posixCodec.printPath(dst)
      doInSession { session =>
        val request = session.newAdhocQuery(
          s"""for $$d in xdmp:directory("$dstUri", "1")
              return xdmp:document-delete(xdmp:node-uri($$d)),
              for $$d in xdmp:directory("$srcUri","1")
              let $$oldName := xdmp:node-uri($$d)
              let $$newName := fn:concat("$dstUri", fn:tokenize($$oldName, "/")[last()])
              return (xdmp:document-insert($$newName, doc($$oldName)), xdmp:document-delete($$oldName))""")
        Task.delay(session.submitRequest(request)).void
      }
    }
  }

  def moveDocuments[S[_]](src: ADir, dst: ADir)(implicit
    S: Task :<: S
  ): Free[S, Unit] =
    lift(moveDocuments_(src, dst)).into[S]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def exists_(path: APath): Task[Boolean] = {
    val uri = posixCodec.printPath(path)
    doInSession { session =>
      val request = session.newAdhocQuery(s"""fn:exists(fn:filter(function ($$uri) { fn:starts-with($$uri, "$uri") }, cts:uris()))""")
      Task.delay(scala.Boolean.unbox(session.submitRequest(request).next.getItem.asInstanceOf[XSBoolean].asBoolean))
    }
  }

  def exists[S[_]](uri: APath)(implicit
    S: Task :<: S
  ): Free[S, Boolean] =
    lift(exists_(uri)).into[S]

  def subDirs_(dir: ADir): Task[ResourceNotFound \/ Set[RDir]] = {
    val uri = posixCodec.printPath(dir)
    doInSession { session =>
      val request = session.newAdhocQuery(
        s"""for $$uri-prop in xdmp:document-properties(cts:uris("$uri"))[.//prop:directory]
              return base-uri($$uri-prop)""")
      val result = Task.delay(session.submitRequest(request).toResultItemArray)
      result.map { resultItems =>
        val absDirs = resultItems.map(i => posixCodec.parseAbsDir(i.getItem.asString)).toList.unite.map(sandboxAbs)
        val relDirs = absDirs.map(_.relativeTo(dir)).unite
        relDirs.toSet.right[ResourceNotFound]
      }
    }
  }

  def subDirs[S[_]](dir: ADir)(implicit
    S: Task :<: S
  ): Free[S, ResourceNotFound \/ Set[RDir]] =
    lift(subDirs_(dir)).into[S]

  def createDir_(dir: ADir): Task[AlreadyExists \/ Unit] = {
    val uri = posixCodec.printPath(dir)
    doInSession { session =>
      val request = session.newAdhocQuery(s"""xdmp:directory-create("$uri")""")
      Task.delay(session.submitRequest(request)).as(().right[AlreadyExists]).handle {
        case ex: XQueryException if ex.getCode == "XDMP-DIREXISTS" => AlreadyExists(dir).left
      }
    }
  }

  def createDir[S[_]](dir: ADir)(implicit
    S0: Task :<: S
  ): Free[S, AlreadyExists \/ Unit] =
    lift(createDir_(dir)).into[S]

  def deleteContent_(dir: ADir): Task[Unit] = {
    val uri = posixCodec.printPath(dir)
    doInSession { session =>
      val request = session.newAdhocQuery(s"""fn:map(xdmp:document-delete, xdmp:directory("$uri"))""")
      Task.delay(session.submitRequest(request)).void
    }
  }

  def deleteContent[S[_]](dir: ADir)(implicit
    S: Task :<: S
  ): Free[S, Unit] =
    lift(deleteContent_(dir)).into[S]

  def deleteStructure_(dir: ADir): Task[Unit] = {
    val uri = posixCodec.printPath(dir)
    doInSession { session =>
      // `directory-delete` also deletes the "Content" (documents in the directory),
      // which we may want to change at some point
      val request = session.newAdhocQuery(s"""xdmp:directory-delete("$uri")""")
      Task.delay(session.submitRequest(request)).void
    }
  }

  def deleteStructure[S[_]](dir: ADir)(implicit
    S: Task :<: S
  ): Free[S, Unit] =
    lift(deleteStructure_(dir)).into[S]

  def writeInDir_(dir: ADir, contents: Vector[String]): Task[Unit] = {
    val createOptions = {
      val copts = new ContentCreateOptions()
      copts.setFormatJson()
      copts
    }

    val seqFmt = {
      val width = ceil(log(contents.size.toDouble) / log(16)).toInt
      if (width === 0) "" else s"%0${width}x"
    }

    def mkContent(cid: String, seqNum: Int, str: String): Content = {
      val fname = cid + seqFmt.format(seqNum)
      val uri = posixCodec.printPath(dir </> file(fname))
      ContentFactory.newContent(uri, str, createOptions)
    }

    doInSession { session =>
      for {
        cid <- chunkId
        cs  =  contents.zipWithIndex map { case (s, i) => mkContent(cid, i, s) }
        _   <- Task.delay(session.insertContent(cs.toArray))
      } yield ()
    }
  }

  def writeInDir[S[_]](dir: ADir, content: Vector[String])(implicit
    S: Task :<: S
  ): Free[S, Unit] =
    lift(writeInDir_(dir, content)).into[S]

  //////

  private def chunkId: Task[String] =
    Task.delay(uuidGenerator.generate)
      .map(id => uuid.toSequentialString(id) getOrElse uuid.toOpaqueString(id))
}

// Is there a better way of doing this without as much duplication?
object Client {
  def readDirectory[S[_]](dir: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Process[Task, ResultItem]] = {
    getClient.asks(_.readDirectory(dir))
  }

  def deleteContent[S[_]](dir: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] = {
    getClient.asksM(_.deleteContent(dir))
  }

  def deleteStructure[S[_]](dir: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] = {
    getClient.asksM(_.deleteStructure(dir))
  }

  def moveDocuments[S[_]](src: ADir, dst: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] = {
    getClient.asksM(_.moveDocuments(src, dst))
  }

  def exists[S[_]](uri: APath)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Boolean] =
    getClient.asksM(_.exists(uri))

  def subDirs[S[_]](dir: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, ResourceNotFound \/ Set[RDir]] =
    getClient.asksM(_.subDirs(dir))

  def createDir[S[_]](dir: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, AlreadyExists \/ Unit] =
    getClient.asksM(_.createDir(dir))

  def writeInDir[S[_]](dir: ADir, content: Vector[String])(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] =
    getClient.asksM(_.writeInDir(dir, content))
}

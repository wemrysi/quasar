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
import quasar.fs._, ManageFile._

import com.marklogic.client._
import com.marklogic.client.io.{InputStreamHandle, StringHandle}
import com.marklogic.xcc._
import com.marklogic.xcc.exceptions.XQueryException
import com.marklogic.xcc.types._

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._

import argonaut._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.stream.io
import jawn._
import jawnstreamz._

sealed trait Error

object Error {
  def fromException(ex: scala.Throwable): Option[Error] = ex match {
    case ex: ResourceNotFoundException => ResourceNotFound(ex.getMessage).some
    case ex: ForbiddenUserException    => Forbidden(ex.getMessage).some
    case ex: FailedRequestException    => FailedRequest(ex.getMessage).some
    case _                             => none
  }
}

final case class AlreadyExists(path: APath) extends Error
final case class ResourceNotFound(message: String) extends Error
final case class Forbidden(message: String) extends Error
final case class FailedRequest(message: String) extends Error

final case class Client(client: DatabaseClient, contentSource: ContentSource) {

  val docManager = client.newJSONDocumentManager
  val newSession: Task[Session] = Task.delay(contentSource.newSession)
  def closeSession(s: Session): Task[Unit] = Task.delay(s.close)
  def doInSession[A](f: Session => Task[A]): Task[A] = for {
    session <- newSession
    result  <- f(session)
    _       <- closeSession(session)
  } yield result

  def release: Task[Unit] = Task.delay(client.release)

  def readDocument_(doc: AFile): Task[ResourceNotFoundException \/ Process[Task, Json]] = {
    val bufferSize = 100
    val uri = posixCodec.printPath(doc)
    val chunkSizes: Process[Task, Int] = Process.constant(64)
    for {
      inputStream <- Task.delay{
                       val buffer = Array.ofDim[Byte](bufferSize)
                       new ByteArrayInputStream(buffer)
                     }
      handle      = new InputStreamHandle(inputStream)
      stream      = chunkSizes.through(io.chunkR(inputStream)).unwrapJsonArray
      exception   <- Task.delay(\/.fromTryCatchThrowable[InputStreamHandle, ResourceNotFoundException](docManager.read(uri, handle)))
    } yield exception.as(stream)
  }

  def readDocument[S[_]](doc: AFile)(implicit
    S: Task :<: S
  ): Free[S, ResourceNotFoundException \/ Process[Task, Json]] =
    lift(readDocument_(doc)).into[S]

  def getQuery(s: String) =
    s"<query>$s</query>"

  def readDirectory(dir: ADir): Process[Task, ResultItem] = {
    val uri = posixCodec.printPath(dir)
    io.iteratorR(newSession)(closeSession) { session =>
      val request = session.newAdhocQuery(s"""cts:search(fn:doc(), cts:directory-query("$uri"))""")
      Task.delay(session.submitRequest(request).iterator.asScala)
    }
  }

  def move_(scenario: MoveScenario, semantics: MoveSemantics): Task[Unit] = scenario match {
    case MoveScenario.FileToFile(src, dst) => ???
    case MoveScenario.DirToDir(src, dst)   => ???
  }

  def move[S[_]](scenario: MoveScenario, semantics: MoveSemantics)(implicit
    S: Task :<: S
  ): Free[S, Unit] =
    lift(move_(scenario, semantics)).into[S]

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

  def write_(uri: String, content: String): Task[Error \/ Unit] =
    Task.delay(docManager.write(uri, new StringHandle(content)))
      .as(().right)
      .handleWith {
        case ex => Error.fromException(ex).cata(err => Task.now(err.left), Task.fail(ex))
      }

  def write[S[_]](uri: String, content: String)(implicit
    S: Task :<: S
  ): Free[S, Error \/ Unit] =
    lift(write_(uri, content)).into[S]

  def writeInDir_(dir: ADir, contents: Vector[String]): Task[Error \/ Unit] =
    doInSession( session =>
      Task.gatherUnordered(contents.map { content =>
        // TODO: Make pure
        val docPath = dir </> file(scala.util.Random.alphanumeric.take(10).mkString)
        val uri = posixCodec.printPath(docPath)
        val createOptions = new ContentCreateOptions()
        createOptions.setFormatJson()
        val toInsert = ContentFactory.newContent(uri, content, createOptions)
        Task.delay(session.insertContent(toInsert))
      }).void.map(_.right))

  def writeInDir[S[_]](dir: ADir, content: Vector[String])(implicit
    S: Task :<: S
  ): Free[S, Error \/ Unit] =
    lift(writeInDir_(dir, content)).into[S]

  //////

  /* Temporary parser until jawn-argonaut supports 6.2.x. */
  @SuppressWarnings(Array(
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var"))
  private implicit val facade: Facade[Json] = {
    new Facade[Json] {
      def jnull() = Json.jNull
      def jfalse() = Json.jFalse
      def jtrue() = Json.jTrue
      def jnum(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jint(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jstring(s: String) = Json.jString(s)

      def singleContext() = new FContext[Json] {
        var value: Json = null
        def add(s: String) = { value = jstring(s) }
        def add(v: Json) = { value = v }
        def finish: Json = value
        def isObj: Boolean = false
      }

      def arrayContext() = new FContext[Json] {
        val vs = scala.collection.mutable.ListBuffer.empty[Json]
        def add(s: String) = { vs += jstring(s); () }
        def add(v: Json) = { vs += v; () }
        def finish: Json = Json.jArray(vs.toList)
        def isObj: Boolean = false
      }

      def objectContext() = new FContext[Json] {
        var key: String = null
        var vs = JsonObject.empty
        def add(s: String): Unit =
          if (key == null) { key = s } else { vs = vs + (key, jstring(s)); key = null }
        def add(v: Json): Unit =
        { vs = vs + (key, v); key = null }
        def finish = Json.jObject(vs)
        def isObj = true
      }
    }
  }
}

// Is there a better way of doing this without as much duplication?
object Client {
  def readDocument[S[_]](doc: AFile)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, ResourceNotFoundException \/ Process[Task, Json]] =
    getClient.asksM(_.readDocument(doc))

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

  def move[S[_]](scenario: MoveScenario, semantics: MoveSemantics)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] = {
    getClient.asksM(_.move(scenario, semantics))
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

  def write[S[_]](uri: String, content: String)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Error \/ Unit] =
    getClient.asksM(_.write(uri, content))

  def writeInDir[S[_]](dir: ADir, content: Vector[String])(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Error \/ Unit] =
    getClient.asksM(_.writeInDir(dir, content))

  def execute[S[_]](xQuery: String, dst: ADir)(implicit
    getClient: Read.Ops[Client, S],
    S: Task :<: S
  ): Free[S, Unit] = ???

}

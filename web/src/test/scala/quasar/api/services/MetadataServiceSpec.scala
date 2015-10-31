package quasar
package api
package services

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import quasar.Predef._
import quasar.fs._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.server._
import scalaz._
import scalaz.concurrent.Task
import pathy.Path._
import quasar.fs.PathyGen._

class MetadataServiceSpec extends Specification with ScalaCheck with FileSystemFixture with Http4s {
  import inmemory._, ManageFile.Node, DataGen._

  def runService(mem: InMemState): ManageFile ~> Task =
    new (ManageFile ~> Task) {
      def apply[A](fs: ManageFile[A]) =
        Task.now(manageFile(fs).eval(mem))
    }

  def service(mem: InMemState): HttpService =
    metadata.service[ManageFileF](Coyoneda.liftTF(runService(mem)))

  import posixCodec.printPath

  "Metadata Service" should {
    "respond with NotFound" >> {
      "if directory does not exist" ! prop { dir: AbsDir[Sandboxed] =>
        val path:String = printPath(dir)
        val response = service(InMemState.empty)(Request(uri = Uri(path = path))).run
        response.status must_== Status.NotFound
        response.as[String].run must_== s"${printPath(dir)}: doesn't exist"
      }

      "file does not exist" >> {
        val unexistantFile = rootDir[Sandboxed] </> dir("foo") </> file("bar.json")
        val path:String = posixCodec.printPath(unexistantFile)
        val response = service(InMemState.empty)(Request(uri = Uri(path = path))).run
        response.status must_== Status.NotFound
        response.as[Json].run must_== jSingleObject("error", jString(s"File not found: $path"))
      }

      "if file with same name as existing directory (without trailing slash)" >> {
        val fileWithDirName = rootDir[Sandboxed] </> dir("foo") </> file("bar")
        // We need a "bogus" file in order to create a "directory" in the in-memory filesystem
        val bogusFile = rootDir[Sandboxed] </> dir("foo") </> dir("bar") </> file("bogus")
        val filesystem = InMemState fromFiles Map(bogusFile -> Vector(Data.Bool(true)))
        val path:String = posixCodec.printPath(fileWithDirName)
        val response = service(filesystem)(Request(uri = Uri(path = path))).run
        response.status must_== Status.NotFound
        response.as[Json].run must_== jSingleObject("error", jString(s"File not found: $path"))
      }
    }

    "respond with empty list for existing empty directory" >>
      todo // The current in-memory filesystem does not support empty directories

    "respond with list of children for existing nonempty directory" ! prop {
      (xss: List[Vector[Data]]) => (xss.nonEmpty) ==> {
        val parentDir = rootDir[Sandboxed] </> dir("foo") </> dir("bar")
        val path:String = posixCodec.printPath(parentDir)
        val filenames = xss.zipWithIndex.map { case (_, i) => parentDir </> file(s"f${i}.txt") }
        val filesystem = InMemState fromFiles filenames.zip(xss).toMap
        val childNodes = filenames.flatMap(_ relativeTo parentDir).map(Node.File).sorted

        service(filesystem)(Request(uri = Uri(path = path)))
          .as[Json].run must_== Json("children" := childNodes)
      }
    }

    "respond with Ok for existing file" ! prop { xs: Vector[Data] =>
      val aFile = rootDir[Sandboxed] </> dir("foo") </> file("bar.json")
      val path: String = posixCodec.printPath(aFile)
      val mem = InMemState fromFiles Map(aFile -> xs)

      service(mem)(Request(uri = Uri(path = path)))
        .as[Json].run must_== Json.obj()
    }
  }
}

package quasar
package api

import quasar.Predef._
import quasar.fs._

import argonaut._, Argonaut._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.server._
import scalaz._
import scalaz.concurrent.Task
import pathy.Path._

class MetadataServiceSpec extends FileSystemSpec with Http4s {
  import inmemory._, ManageFile.Node, DataGen._

  def runService(mem: InMemState): ManageFile ~> Task =
    new (ManageFile ~> Task) {
      def apply[A](fs: ManageFile[A]) =
        Task.now(manageFile(fs).eval(mem))
    }

  def service(mem: InMemState): HttpService =
    metadata.service[ManageFileF](Coyoneda.liftTF(runService(mem)))

  "Metadata Service" should {
    // NB: The existing metadata service returns empty lists in these cases
    "respond with NotFound" >> {
      "directory does not exist" >> todo

      "file does not exist" >> todo

      "file with same name as existing directory (without trailing slash)" >> todo
    }

    "respond with empty list for existing empty directory" >> todo

    "respond with list of children for existing nonempty directory" ! prop {
      (xss: List[Vector[Data]]) => (xss.nonEmpty) ==> {
        val d = rootDir[Sandboxed] </> dir("foo") </> dir("bar")
        val s = posixCodec.printPath(d)
        val filenames = xss.zipWithIndex.map { case (_, i) => d </> file(s"f${i}.txt") }
        val mem = InMemState fromFiles filenames.zip(xss).toMap
        val childNodes = filenames.flatMap(_ relativeTo d).map(Node.File).sorted

        service(mem)(Request(uri = Uri(path = s)))
          .as[Json].run must_== Json("children" := childNodes)
      }
    }

    "respond with Ok for existing file" ! prop { xs: Vector[Data] =>
      val f = rootDir[Sandboxed] </> dir("foo") </> file("bar.json")
      val s = posixCodec.printPath(f)
      val mem = InMemState fromFiles Map(f -> xs)

      service(mem)(Request(uri = Uri(path = s)))
        .as[Json].run must_== Json.obj()
    }
  }
}

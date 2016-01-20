package quasar.api

import org.specs2.ScalaCheck
import org.specs2.scalaz.ScalazMatchers
import pathy.Path
import quasar.Predef._

import org.specs2.mutable._
import quasar.fs.{Natural, Positive}

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream.{Process}

import scodec.bits._
import scodec.interop.scalaz._

import pathy.Path._
import quasar.fs.NumericArbitrary._
import pathy.scalacheck.PathyArbitrary._

class ZipSpecs extends Specification with ScalaCheck with ScalazMatchers {
  args.report(showtimes=true)

  "zipFiles" should {
    import Zip._

    def rand = new java.util.Random
    def randBlock = Array.fill[Byte](1000)(rand.nextInt.toByte)

    def f4: Process[Task, ByteVector] = {
      val block = ByteVector.view(randBlock)
      Process.emitAll(Vector.fill(1000)(block))
    }

    def unzip[A](f: java.io.InputStream => A)(p: Process[Task, ByteVector]): Task[List[(RelFile[Sandboxed], A)]] =
      Task.delay {
        val bytes = p.runLog.run.toList.concatenate  // FIXME: this means we can't use this to test anything big
        val is = new java.io.ByteArrayInputStream(bytes.toArray)
        val zis = new java.util.zip.ZipInputStream(is)
        Stream.continually(zis.getNextEntry).takeWhile(_ != null).map { entry =>
          (posixCodec.parseRelFile(entry.getName).get relativeTo currentDir[Sandboxed]).get -> f(zis)
        }.toList
      }

    // For testing, capture all the bytes from a process, parse them with a
    // ZipInputStream, and capture just the size of the contents of each file.
    def counts(p: Process[Task, ByteVector]): Task[List[(RelFile[Sandboxed], Natural)]] = {
      def count(is: java.io.InputStream): Natural = {
        def loop(n: Natural): Natural = if (is.read ≟ -1) n else loop(n + Natural._1)
        loop(Natural._0)
      }
      unzip(count)(p)
    }

    def bytes(p: Process[Task, ByteVector]): Task[List[(RelFile[Sandboxed], ByteVector)]] = {
      def read(is: java.io.InputStream): ByteVector = {
        def loop(acc: ByteVector): ByteVector = {
          val buffer = new Array[Byte](16*1024)
          val n = is.read(buffer)
          if (n <= 0) acc else loop(acc ++ ByteVector.view(buffer).take(n))
        }
        loop(ByteVector.empty)
      }
      unzip(read)(p)
    }

    "zip files of constant bytes" ! prop { (filesAndSize: Map[RelFile[Sandboxed], Positive], byte: Byte) =>
      def byteStream(size: Positive): Process[Task, ByteVector] =
        Process.emit(ByteVector.view(Array.fill(size.toInt)(byte)))
      val bytesMapping = filesAndSize.mapValues(byteStream)
      val z = zipFiles(bytesMapping.toList)
      counts(z).run must equal(filesAndSize.mapValues(_.toNatural).toList)
    }.set(minTestsOk = 10) // This test is relatively slow

    "zip files of random bytes" ! prop { filesAndSize: Map[RelFile[Sandboxed], Positive] =>
      def byteStream(size: Positive): Process[Task, ByteVector] =
        Process.emit(ByteVector.view(Array.fill(size.toInt)(rand.nextInt.toByte)))
      val bytesMapping = filesAndSize.mapValues(byteStream)
      val z = zipFiles(bytesMapping.toList)
      counts(z).run must equal(filesAndSize.mapValues(_.toNatural).toList)
    }.set(minTestsOk = 10) // This test is relatively slow

    "zip many large files of random bytes (100 MB)" in {
      // NB: this is mainly a performance check. Right now it's about 2 seconds for 100 MB for me.
      val Files = 100
      val RawSize = Files*1000*1000L
      val MinExpectedSize = (RawSize*0.005).toInt
      val MaxExpectedSize = (RawSize*0.010).toInt

      val paths = (0 until Files).toList.map(i => file[Sandboxed]("foo" + i))
      val z = zipFiles(paths.map(_ -> f4))

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.run(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "zip many large files of random bytes (10 GB)" in {
      // NB: comment this to verify the heap is not consumed
      skipped("too slow to run every time (~2 minutes)")

      val Files = 10*1000
      val RawSize = Files*1000*1000L
      val MinExpectedSize = (RawSize*0.005).toInt
      val MaxExpectedSize = (RawSize*0.010).toInt

      val paths = (0 until Files).toList.map(i => file[Sandboxed]("foo" + i))
      val z = zipFiles(paths.map(_ -> f4))

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.run(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "read twice without conflict" ! prop { filesAndSize: Map[RelFile[Sandboxed], Positive] =>
      def byteStream(size: Positive): Process[Task, ByteVector] =
        Process.emit(ByteVector.view(Array.fill(size.toInt)(rand.nextInt.toByte)))
      val bytesMapping = filesAndSize.mapValues(byteStream)
      val z = zipFiles(bytesMapping.toList)
      bytes(z).run must equal(bytes(z).run)
    }.set(minTestsOk = 10) // This test is relatively slow
  }
}

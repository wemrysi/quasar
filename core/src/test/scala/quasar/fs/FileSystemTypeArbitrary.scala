package quasar.fs

import org.scalacheck._

trait FileSystemTypeArbitrary {
  implicit val fileSystemTypeArbitrary: Arbitrary[FileSystemType] =
    Arbitrary(Gen.alphaStr.map(FileSystemType(_)))
}

object FileSystemTypeArbitrary extends FileSystemTypeArbitrary

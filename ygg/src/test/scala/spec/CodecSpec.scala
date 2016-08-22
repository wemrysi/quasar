package ygg.tests

import blueeyes._
import TestSupport._
import org.scalacheck.Shrink
import ygg.data._
import ByteBufferPool._

class CodecSpec extends quasar.Qspec {
  val pool      = ByteBufferPool()
  val smallPool = new ByteBufferPool(10)

  implicit def arbBitSet: Arbitrary[BitSet]                                 = Arbitrary(genBitSet)
  implicit def arbSparseBitSet: Arbitrary[Codec[BitSet] -> BitSet]          = Arbitrary(genSparseBitSet)
  implicit def arbSparseRawBitSet: Arbitrary[Codec[RawBitSet] -> RawBitSet] = Arbitrary(genSparseRawBitSet)

  def surviveEasyRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val buf = pool.acquire
    codec.writeUnsafe(a, buf)
    buf.flip()
    codec.read(buf) must_== a
  }
  def surviveHardRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val bytes = smallPool.run(for {
      _     <- codec.write(a)
      bytes <- flipBytes
      _     <- release
    } yield bytes)
    bytes.length must_== codec.encodedSize(a)
    codec.read(byteBuffer(bytes)) must_== a
  }
  def surviveRoundTrip[A](codec: Codec[A])(implicit a: Arbitrary[A], s: Shrink[A]) = "survive round-trip" should {
    "with large buffers" in prop((a: A) => surviveEasyRoundTrip(a)(codec))
    "with small buffers" in prop((a: A) => surviveHardRoundTrip(a)(codec))
  }

  "constant codec" should {
    "write 0 bytes" in {
      val codec = Codec.ConstCodec(true)
      codec.encodedSize(true) must_== 0
      codec.read(byteBuffer(new Array[Byte](0))) must_== true
      codec.writeUnsafe(true, java.nio.ByteBuffer.allocate(0))
      ok
    }
  }
  "LongCodec" should surviveRoundTrip(Codec.LongCodec)
  "PackedLongCodec" should surviveRoundTrip(Codec.PackedLongCodec)
  "BooleanCodec" should surviveRoundTrip(Codec.BooleanCodec)
  "DoubleCodec" should surviveRoundTrip(Codec.DoubleCodec)
  "Utf8Codec" should surviveRoundTrip(Codec.Utf8Codec)
  "BigDecimalCodec" should surviveRoundTrip(Codec.BigDecimalCodec)
  "BitSetCodec" should surviveRoundTrip(Codec.BitSetCodec)
  "SparseBitSet" should {
    "survive round-trip" should {
      "with large buffers" in {
        prop((sparse: (Codec[BitSet], BitSet)) => surviveEasyRoundTrip(sparse._2)(sparse._1))
      }
      "with small buffers" in {
        prop((sparse: (Codec[BitSet], BitSet)) => surviveHardRoundTrip(sparse._2)(sparse._1))
      }
    }
  }
  "SparseRawBitSet" should {
    "survive round-trip" should {
      "with large buffers" in {
        prop((sparse: (Codec[RawBitSet], RawBitSet)) => surviveEasyRoundTrip(sparse._2)(sparse._1))
      }
      "with small buffers" in {
        prop((sparse: (Codec[RawBitSet], RawBitSet)) => surviveHardRoundTrip(sparse._2)(sparse._1))
      }
    }
  }
  "IndexedSeqCodec" should {
    "survive round-trip" should {
      "with large buffers" in {
        prop(surviveEasyRoundTrip(_: IndexedSeq[Long]))
        prop(surviveEasyRoundTrip(_: IndexedSeq[IndexedSeq[Long]]))
        prop(surviveEasyRoundTrip(_: IndexedSeq[String]))
      }
      "with small buffers" in {
        prop(surviveHardRoundTrip(_: IndexedSeq[Long]))
        prop(surviveHardRoundTrip(_: IndexedSeq[IndexedSeq[Long]]))
        prop(surviveHardRoundTrip(_: IndexedSeq[String]))
      }
    }
  }
  "ArrayCodec" should {
    "survive round-trip" should {
      "with large buffers" in {
        prop(surviveEasyRoundTrip(_: Array[Long]))
        prop(surviveEasyRoundTrip(_: Array[Array[Long]]))
        prop(surviveEasyRoundTrip(_: Array[String]))
      }
      "with small buffers" in {
        prop(surviveHardRoundTrip(_: Array[Long]))
        prop(surviveHardRoundTrip(_: Array[Array[Long]]))
        prop(surviveHardRoundTrip(_: Array[String]))
      }
    }
  }
}

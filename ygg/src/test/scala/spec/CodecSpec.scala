package ygg.tests

import ygg.common._
import TestSupport._
import org.scalacheck.Shrink
import ygg.data._
import ByteBufferPool._

class CodecSpec extends quasar.Qspec {
  val pool      = ByteBufferPool()
  val smallPool = new ByteBufferPool(10)

  implicit private def arbBitSet: Arbitrary[BitSet]                                 = Arbitrary(genBitSet)
  implicit private def arbSparseBitSet: Arbitrary[Codec[BitSet] -> BitSet]          = Arbitrary(genSparseBitSet)
  implicit private def arbSparseRawBitSet: Arbitrary[Codec[RawBitSet] -> RawBitSet] = Arbitrary(genSparseRawBitSet)

  private def surviveEasyRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val buf = pool.acquire
    codec.writeUnsafe(a, buf)
    buf.flip()
    buf.read[A] must_=== a
  }
  private def surviveHardRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val bytes = smallPool.run(for {
      _     <- codec.write(a)
      bytes <- flipBytes
      _     <- release
    } yield bytes)

    bytes.length must_=== codec.encodedSize(a)
    byteBuffer(bytes).read[A] must_=== a
  }
  private def surviveRoundTrip[A](codec: Codec[A])(implicit a: Arbitrary[A], s: Shrink[A]) = "survive round-trip" >> {
    "with large buffers" in prop((a: A) => surviveEasyRoundTrip(a)(codec))
    "with small buffers" in prop((a: A) => surviveHardRoundTrip(a)(codec))
  }

  "constant codec" >> {
    "write 0 bytes" in {
      val codec = Codec.ConstCodec(true)
      codec.encodedSize(true) must_== 0
      byteBuffer(new Array[Byte](0)).read[Boolean](codec) must_== true
      codec.writeUnsafe(true, java.nio.ByteBuffer.allocate(0))
      ok
    }
  }
  "LongCodec" >> surviveRoundTrip(Codec.LongCodec)
  "PackedLongCodec" >> surviveRoundTrip(Codec.PackedLongCodec)
  "BooleanCodec" >> surviveRoundTrip(Codec.BooleanCodec)
  "DoubleCodec" >> surviveRoundTrip(Codec.DoubleCodec)
  "Utf8Codec" >> surviveRoundTrip(Codec.Utf8Codec)
  "BigDecimalCodec" >> surviveRoundTrip(Codec.BigDecimalCodec)
  "BitSetCodec" >> surviveRoundTrip(Codec.BitSetCodec)
  "SparseBitSet" >> {
    "survive round-trip" >> {
      "with large buffers" in {
        prop((sparse: (Codec[BitSet], BitSet)) => surviveEasyRoundTrip(sparse._2)(sparse._1))
      }
      "with small buffers" in {
        prop((sparse: (Codec[BitSet], BitSet)) => surviveHardRoundTrip(sparse._2)(sparse._1))
      }
    }
  }
  "SparseRawBitSet" >> {
    "survive round-trip" >> {
      "with large buffers" in {
        prop((sparse: (Codec[RawBitSet], RawBitSet)) => surviveEasyRoundTrip(sparse._2)(sparse._1))
      }
      "with small buffers" in {
        prop((sparse: (Codec[RawBitSet], RawBitSet)) => surviveHardRoundTrip(sparse._2)(sparse._1))
      }
    }
  }
  "IndexedSeqCodec" >> {
    "survive round-trip" >> {
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
  "ArrayCodec" >> {
    "survive round-trip" >> {
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

package ygg.data

import blueeyes._
import ygg.table._

/**
  * Defines a base set of codecs that are often used in `RowFormat`s.
  */
trait StdCodecs {
  implicit def LongCodec: Codec[Long]
  implicit def DoubleCodec: Codec[Double]
  implicit def BigDecimalCodec: Codec[BigDecimal]
  implicit def StringCodec: Codec[String]
  implicit def BooleanCodec: Codec[Boolean]
  implicit def DateTimeCodec: Codec[DateTime]
  implicit def PeriodCodec: Codec[Period]
  implicit def BitSetCodec: Codec[BitSet]
  implicit def RawBitSetCodec: Codec[RawBitSet]
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: CTag[A]): Codec[Array[A]]

  def codecForCValueType[A](cType: CValueType[A]): Codec[A] = cType match {
    case CBoolean             => BooleanCodec
    case CString              => StringCodec
    case CLong                => LongCodec
    case CDouble              => DoubleCodec
    case CNum                 => BigDecimalCodec
    case CDate                => DateTimeCodec
    case CPeriod              => PeriodCodec
    case CArrayType(elemType) => ArrayCodec(codecForCValueType(elemType), elemType.classTag)
  }
}

trait RowFormatCodecs extends StdCodecs { self: RowFormat =>
  implicit def LongCodec: Codec[Long]             = Codec.PackedLongCodec
  implicit def DoubleCodec: Codec[Double]         = Codec.DoubleCodec
  implicit def BigDecimalCodec: Codec[BigDecimal] = Codec.BigDecimalCodec
  implicit def StringCodec: Codec[String]         = Codec.Utf8Codec
  implicit def BooleanCodec: Codec[Boolean]       = Codec.BooleanCodec
  implicit def DateTimeCodec: Codec[DateTime]     = Codec.DateCodec
  implicit def PeriodCodec: Codec[Period]         = Codec.PeriodCodec
  // implicit def BitSetCodec: Codec[BitSet] = Codec.BitSetCodec
  //@transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val BitSetCodec: Codec[BitSet]                               = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val RawBitSetCodec: Codec[RawBitSet]                         = Codec.SparseRawBitSetCodec(columnRefs.size)
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]   = Codec.IndexedSeqCodec(elemCodec)
  implicit def ArrayCodec[A](implicit elemCodec: Codec[A], m: CTag[A]): Codec[Array[A]] = Codec.ArrayCodec(elemCodec)(m)
}

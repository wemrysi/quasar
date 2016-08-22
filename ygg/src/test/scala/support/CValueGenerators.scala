package ygg.tests

import ygg.common._
import TestSupport._
import Gen.{ containerOfN, frequency, alphaStr, listOfN, identifier, oneOf, delay }
import ygg.json._
import ygg.table._

object CValueGenerators {
  type JSchema = Seq[JPath -> CType]

  def inferSchema(data: Seq[JValue]): JSchema = data match {
    case Seq() => Seq()
    case hd +: tl =>
      val current = hd.flattenWithPath flatMap { case (path, jv) => CType.forJValue(jv) map (path -> _) }
      current ++ inferSchema(tl) distinct
  }
}

trait CValueGenerators {
  import CValueGenerators._

  def schema(depth: Int): Gen[JSchema] =
    if (depth <= 0) leafSchema
    else oneOf(delay(objectSchema(depth, choose(1, 3))), delay(arraySchema(depth, choose(1, 5))), leafSchema)

  def objectSchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] = {
    for {
      size       <- sizeGen
      names      <- containerOfN[Set, String](size, identifier)
      subschemas <- listOfN(size, schema(depth - 1))
    } yield {
      for {
        (name, subschema) <- names.toList zip subschemas
        (jpath, ctype)    <- subschema
      } yield {
        (JPathField(name) \ jpath, ctype)
      }
    }
  }

  def arraySchema(depth: Int, sizeGen: Gen[Int]): Gen[JSchema] =
    sizeGen >> { size =>
      schema(depth - 1) * size ^^ { subschemas =>
        0 until size zip subschemas flatMap {
          case (idx, pairs) =>
            pairs map {
              case (jpath, ctype) =>
                (JPathIndex(idx) \ jpath) -> ctype
            }
        }
      }
    }

  def leafSchema: Gen[JSchema] = ctype map { t =>
    (NoJPath -> t) :: Nil
  }

  def ctype: Gen[CType] = oneOf(
    CString,
    CBoolean,
    CLong,
    CDouble,
    CNum,
    CNull,
    CEmptyObject,
    CEmptyArray
  )

  // FIXME: TODO Should this provide some form for CDate?
  def jvalue(ctype: CType): Gen[JValue] = ctype match {
    case CString       => alphaStr map (JString(_))
    case CBoolean      => genBool map (JBool(_))
    case CLong         => genLong map (ln => JNum(decimal(ln)))
    case CDouble       => genDouble map (d => JNum(decimal(d)))
    case CNum          => genBigDecimal map (bd => JNum(bd))
    case CNull         => JNull
    case CEmptyObject  => JObject.empty
    case CEmptyArray   => JArray.empty
    case CUndefined    => JUndefined
    case CArrayType(_) => abort("undefined")
    case CDate         => abort("undefined")
    case CPeriod       => abort("undefined")
  }

  def jvalue(schema: Seq[JPath -> CType]): Gen[JValue] = {
    schema.foldLeft(Gen.const[JValue](JUndefined)) {
      case (gen, (jpath, ctype)) =>
        for {
          acc <- gen
          jv  <- jvalue(ctype)
        } yield {
          acc.unsafeInsert(jpath, jv)
        }
    }
  }

  def genEventColumns(jschema: JSchema): Gen[Int -> Stream[Identities -> Seq[JPath -> JValue]]] =
    for {
      idCount     <- choose(1, 3)
      dataSize    <- choose(0, 20)
      ids         <- setOfN[List[Long]](dataSize, listOfN[Long](idCount, genPosLong))
      values      <- listOfN[Seq[JPath -> JValue]](dataSize, Gen.sequence(jschema map { case (k, v) => jvalue(v) map (k -> _) }))
      falseDepth  <- choose(1, 3)
      falseSchema <- schema(falseDepth)
      falseSize   <- choose(0, 5)
      falseIds    <- setOfN[List[Long]](falseSize, listOfN(idCount, genPosLong))
      falseValues <- listOfN[Seq[JPath -> JValue]](falseSize, Gen.sequence(falseSchema map { case (k, v) => jvalue(v).map(k -> _) }))

      falseIds2 = falseIds -- ids // distinct ids
    } yield {
      (idCount, (ids.map(_.toArray) zip values).toStream ++ (falseIds2.map(_.toArray) zip falseValues).toStream)
    }

  def assemble(parts: Seq[JPath -> JValue]): JValue = {
    val result = parts.foldLeft[JValue](JUndefined) {
      case (acc, (selector, jv)) => acc.unsafeInsert(selector, jv)
    }

    if (result != JUndefined || parts.isEmpty) result else abort("Cannot build object from " + parts)
  }
}

trait JdbmCValueGenerators {
  def maxArrayDepth = 3

  def genColumn(size: Int, values: Gen[Array[CValue]]): Gen[List[Seq[CValue]]] = containerOfN[List, Seq[CValue]](size, values.map(_.toSeq))

  private def genNonArrayCValueType: Gen[CValueType[_]] = Gen.oneOf[CValueType[_]](CString, CBoolean, CLong, CDouble, CNum, CDate)

  def genCValueType(maxDepth: Int = maxArrayDepth, depth: Int = 0): Gen[CValueType[_]] = {
    if (depth >= maxDepth) genNonArrayCValueType
    else {
      frequency(0 -> (genCValueType(maxDepth, depth + 1) map (CArrayType(_))), 6 -> genNonArrayCValueType)
    }
  }

  def genCType: Gen[CType] = frequency(7 -> genCValueType(), 3 -> Gen.oneOf(CNull, CEmptyObject, CEmptyArray))

  def genValueForCValueType[A](cType: CValueType[A]): Gen[CWrappedValue[A]] = cType match {
    case CString  => genString map (CString(_))
    case CBoolean => genBool map (CBoolean(_))
    case CLong    => genLong map (CLong(_))
    case CDouble  => genDouble map (CDouble(_))
    case CNum =>
      for {
        scale  <- genInt
        bigInt <- genBigInt
      } yield CNum(decimal(bigInt.bigInteger, scale - 1))

    case CDate => genPosLong ^^ (n => CDate(dateTime fromMillis n))
    case CArrayType(elemType) =>
      vectorOf(genValueForCValueType(elemType) map (_.value)) map { xs =>
        CArray(xs.toArray(elemType.classTag), CArrayType(elemType))
      }
    case CPeriod => abort("undefined")
  }

  def genCValue(tpe: CType): Gen[CValue] = tpe match {
    case tpe: CValueType[_] => genValueForCValueType(tpe)
    case CNull              => Gen.const(CNull)
    case CEmptyObject       => Gen.const(CEmptyObject)
    case CEmptyArray        => Gen.const(CEmptyArray)
    case invalid            => abort("No values for type " + invalid)
  }
}

/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir

import quasar.mimir.StdLib.ConstantStrColumn
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._

trait TypeLibModule extends ColumnarTableLibModule {
  trait TypeLib extends ColumnarTableLib {
    import trans._

    override def _lib1 = super._lib1 ++ Set(
      typeOf,
      isNumber,
      isBoolean,
      isNull,
      isString,
      isObject,
      isArray)

    object typeOf extends Op1F1 {
      val _number = "number"
      val _boolean = "boolean"
      val _string = "string"
      val _null = "null"
      val _offsetdatetime = "offsetdatetime"
      val _offsetdate= "offsetdate"
      val _offsettime = "offsettime"
      val _localdatetime = "localdatetime"
      val _localdate= "localdate"
      val _localtime = "localtime"
      val _interval = "interval"
      val _emptyarray = "emptyarray"
      val _emptyobject = "emptyobject"

      val tpe = UnaryOperationType(JType.JUniverseT, JTextT)
      def f1: F1 = CF1P {
        case c: LongColumn => new ConstantStrColumn(c, _number)
        case c: DoubleColumn => new ConstantStrColumn(c, _number)
        case c: NumColumn => new ConstantStrColumn(c, _number)
        case c: BoolColumn => new ConstantStrColumn(c, _boolean)
        case c: StrColumn => new ConstantStrColumn(c, _string)
        case c: NullColumn => new ConstantStrColumn(c, _null)
        case c: OffsetDateTimeColumn => new ConstantStrColumn(c, _offsetdatetime)
        case c: OffsetDateColumn => new ConstantStrColumn(c, _offsetdate)
        case c: OffsetTimeColumn => new ConstantStrColumn(c, _offsettime)
        case c: LocalDateTimeColumn => new ConstantStrColumn(c, _localdatetime)
        case c: LocalDateColumn => new ConstantStrColumn(c, _localdate)
        case c: LocalTimeColumn => new ConstantStrColumn(c, _localtime)
        case c: IntervalColumn => new ConstantStrColumn(c, _interval)
        case c: EmptyArrayColumn => new ConstantStrColumn(c, _emptyarray)
        case c: EmptyObjectColumn => new ConstantStrColumn(c, _emptyobject)
      }
    }

    object isNumber extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNumberT)
    }

    object isBoolean extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JBooleanT)
    }

    object isNull extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JNullT)
    }

    object isString extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JTextT)
    }

    object isObject extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JObjectUnfixedT)
    }

    object isArray extends Op1 {
      val tpe = UnaryOperationType(JType.JUniverseT, JBooleanT)
      def spec[A <: SourceType](source: TransSpec[A]): TransSpec[A] =
        trans.IsType(source, JArrayUnfixedT)
    }
  }
}

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

package quasar.physical.mongodb

import quasar.Predef._
import quasar._, Planner._, Type.{Const => _, Coproduct => _, _}
import quasar.fp._
import quasar.fs.QueryFile
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}
import quasar.namegen._
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.qscript._
import quasar.std.StdLib._, string._ // TODO: remove this
import javascript._

import matryoshka._, Recursive.ops._, TraverseT.ops._
import org.threeten.bp.Instant
import scalaz._, Scalaz._

object MongoDbQScriptPlanner {
  type Partial[In, Out] = (PartialFunction[List[In], Out], List[InputFinder])

  type OutputM[A] = PlannerError \/ A

  type PartialJs = Partial[JsFn, JsFn]

  def generateTypeCheck[In, Out](or: (Out, Out) => Out)(f: PartialFunction[Type, In => Out]):
      Type => Option[In => Out] =
        typ => f.lift(typ).fold(
          typ match {
            case Type.Interval => generateTypeCheck(or)(f)(Type.Dec)
            case Type.Arr(_) => generateTypeCheck(or)(f)(Type.AnyArray)
            case Type.Timestamp
               | Type.Timestamp ⨿ Type.Date
               | Type.Timestamp ⨿ Type.Date ⨿ Type.Time =>
              generateTypeCheck(or)(f)(Type.Date)
            case Type.Timestamp ⨿ Type.Date ⨿ Type.Time ⨿ Type.Interval =>
              // Just repartition to match the right cases
              generateTypeCheck(or)(f)(Type.Interval ⨿ Type.Date)
            case Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str ⨿ (Type.Timestamp ⨿ Type.Date ⨿ Type.Time) ⨿ Type.Bool =>
              // Just repartition to match the right cases
              generateTypeCheck(or)(f)(
                Type.Int ⨿ Type.Dec ⨿ Type.Interval ⨿ Type.Str ⨿ (Type.Date ⨿ Type.Bool))
            case a ⨿ b =>
              (generateTypeCheck(or)(f)(a) ⊛ generateTypeCheck(or)(f)(b))(
                (a, b) => ((expr: In) => or(a(expr), b(expr))))
            case _ => None
          })(
          Some(_))

  def processMapFuncExpr[T[_[_]]: Recursive: ShowT, A](
    fm: Free[MapFunc[T, ?],  A])(
    recovery: A => OutputM[Expression]):
      OutputM[Expression] =
    freeCataM(fm)(
      interpretM[OutputM, MapFunc[T, ?], A, Expression](
        recovery,
        expression))

  def processMapFunc[T[_[_]]: Recursive: ShowT, A](
    fm: Free[MapFunc[T, ?],  A])(
    recovery: A => JsFn):
      OutputM[JsFn] =
    freeCata(fm)(
      interpret[MapFunc[T, ?], A, OutputM[PartialJs]](
        recovery ⋙ (r => (({ case Nil => r }, List[InputFinder]()): PartialJs).right[PlannerError]),
        javascript)) >>= (_._1.lift(Nil) \/> InternalError("failed JS"))

  // TODO: Should have a JsFn version of this for $reduce nodes.
  val accumulator: ReduceFunc[Expression] => AccumOp[Expression] = {
    import quasar.qscript.ReduceFuncs._

    {
      case Arbitrary(a)     => $first(a)
      case Avg(a)           => $avg(a)
      case Count(_)         => $sum($literal(Bson.Int32(1)))
      case Max(a)           => $max(a)
      case Min(a)           => $min(a)
      case Sum(a)           => $sum(a)
      case UnshiftArray(a)  => $push(a)
      case UnshiftMap(k, v) => ???
    }
  }

  def expression[T[_[_]]: Recursive: ShowT]:
      AlgebraM[OutputM, MapFunc[T, ?], Expression] = {
    import MapFuncs._

    {
      case Nullary(v1) =>
        v1.cataM(BsonCodec.fromEJson).bimap(
          κ(NonRepresentableEJson(v1.shows)),
          $literal(_))
      case Length(a1) => $size(a1).right
      case Date(a1) => ???
      case Time(a1) => ???
      case Timestamp(a1) => ???
      case Interval(a1) => ???
      case TimeOfDay(a1) => ???
      case ToTimestamp(a1) =>
        $add($literal(Bson.Date(Instant.ofEpochMilli(0))), a1).right
      case Extract(a1, a2) => a1.project match {
        case $literalF(Bson.Text(field)) => field match {
          case "century" => $divide($year(a2), $literal(Bson.Int32(100))).right
          case "day" => $dayOfMonth(a2).right
          case "decade" => $divide($year(a2), $literal(Bson.Int32(10))).right
          case "dow" => $add($dayOfWeek(a2), $literal(Bson.Int32(-1))).right
          case "doy" => $dayOfYear(a2).right
          // TODO: epoch
          case "hour" => $hour(a2).right
          case "isodow" =>
            $cond($eq($dayOfWeek(a2), $literal(Bson.Int32(1))),
              $literal(Bson.Int32(7)),
              $add($dayOfWeek(a2), $literal(Bson.Int32(-1)))).right
          // TODO: isoyear
          case "microseconds" =>
            $multiply($millisecond(a2), $literal(Bson.Int32(1000))).right
          case "millennium" =>
            $divide($year(a2), $literal(Bson.Int32(1000))).right
          case "milliseconds" => $millisecond(a2).right
          case "minute"       => $minute(a2).right
          case "month"        => $month(a2).right
          case "quarter"      => // TODO: handle leap years
            $add(
              $divide($dayOfYear(a2), $literal(Bson.Int32(92))),
              $literal(Bson.Int32(1))).right
          case "second"       => $second(a2).right
          // TODO: timezone, timezone_hour, timezone_minute
          case "week"         => $week(a2).right
          case "year"         => $year(a2).right
          case _              =>
            InternalError(field + " is not a valid time period").left
        }
        case _ => InternalError("meh").left
      }
      case Negate(a1)       => $multiply($literal(Bson.Int32(-1)), a1).right
      case Add(a1, a2)      => $add(a1, a2).right
      case Multiply(a1, a2) => $multiply(a1, a2).right
      case Subtract(a1, a2) => $subtract(a1, a2).right
      case Divide(a1, a2)   => $divide(a1, a2).right
      case Modulo(a1, a2)   => $mod(a1, a2).right
      case Power(a1, a2)    => ???

      case Not(a1)     => $not(a1).right
      case Eq(a1, a2)  => $eq(a1, a2).right
      case Neq(a1, a2) => $neq(a1, a2).right
      case Lt(a1, a2)  => $lt(a1, a2).right
      case Lte(a1, a2) => $lte(a1, a2).right
      case Gt(a1, a2)  => $gt(a1, a2).right
      case Gte(a1, a2) => $gte(a1, a2).right
      case IfUndefined(a1, a2) => ???
      case And(a1, a2) => $and(a1, a2).right
      case Or(a1, a2)  => $or(a1, a2).right
      case Coalesce(a1, a2) => $ifNull(a1, a2).right
      case Between(a1, a2, a3) => $and($lte(a2, a1), $lte(a1, a3)).right
      case Cond(a1, a2, a3) => $cond(a1, a2, a3).right

      case Within(a1, a2) => ???

      case Lower(a1) => $toLower(a1).right
      case Upper(a1) => $toUpper(a1).right
      case Bool(a1) =>
        $cond($eq(a1, $literal(Bson.Text("true"))),
          $literal(Bson.Bool(true)),
          $cond($eq(a1, $literal(Bson.Text("false"))),
            $literal(Bson.Bool(false)),
            $literal(Bson.Undefined))).right
      case Integer(a1) => ???
      case Decimal(a1) => ???
      case Null(a1) =>
        $cond($eq(a1, $literal(Bson.Text("null"))),
          $literal(Bson.Null),
          $literal(Bson.Undefined)).right
      case ToString(a1) => ???
      case Search(a1, a2, a3) => ???
      case Substring(a1, a2, a3) => $substr(a1, a2, a3).right

      case MakeArray(a1) => ???
      case MakeMap(a1, a2) => ???
      case ConcatArrays(a1, a2) => $concat(a1, a2).right
      case ConcatMaps(a1, a2) => ???
      case ProjectField($var(DocField(base)), $literal(Bson.Text(field))) =>
        $var(DocField(base \ BsonField.Name(field))).right
      case ProjectIndex(a1, a2)  => ???
      case DeleteField(a1, a2)  => ???

      // NB: This is maybe a NOP for Expressions, as they (all?) safely
      //     short-circuit when given the wrong type. However, our guards may be
      //     more restrictive than the operation, in which case we still want to
      //     short-circuit, so …
      case Guard(expr, typ, cont, fallback) => ???

      case DupArrayIndices(_) => ???
      case DupMapKeys(_)      => ???
      case Range(_, _)        => ???
      case ZipArrayIndices(_) => ???
      case ZipMapKeys(_)      => ???
    }
  }

  def javascript[T[_[_]]: Recursive: ShowT]:
      Algebra[MapFunc[T, ?], OutputM[PartialJs]] = {
    type Output = OutputM[PartialJs]

    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    val HasJs: Output => OutputM[PartialJs] =
      _ <+> \/-(({ case List(field) => field }, List(Here)))

    val HasStr: Output => OutputM[String] = _.flatMap {
      _._1(Nil)(ident("_")) match {
        case Literal(Js.Str(str)) => str.right
        case x => FuncApply("", "JS string", x.toString).left
      }
    }

    def Arity1(a1: Output)(f: JsCore => JsCore): Output =
      HasJs(a1).map {
        case (f1, p1) => ({ case list => JsFn(JsFn.defaultName, f(f1(list)(Ident(JsFn.defaultName)))) }, p1.map(There(0, _)))
      }

    def Arity2(a1: Output, a2: Output)(f: (JsCore, JsCore) => JsCore): Output =
      (HasJs(a1) ⊛ HasJs(a2)) {
        case ((f1, p1), (f2, p2)) =>
          ({ case list => JsFn(JsFn.defaultName, f(f1(list.take(p1.size))(Ident(JsFn.defaultName)), f2(list.drop(p1.size))(Ident(JsFn.defaultName)))) },
            p1.map(There(0, _)) ++ p2.map(There(1, _)))
      }

    def Arity3(a1: Output, a2: Output, a3: Output)(f: (JsCore, JsCore, JsCore) => JsCore): Output =
      (HasJs(a1) ⊛ HasJs(a2) ⊛ HasJs(a3)) {
        case ((f1, p1), (f2, p2), (f3, p3)) =>
          ({ case list => JsFn(JsFn.defaultName, f(
            f1(list.take(p1.size))(Ident(JsFn.defaultName)),
            f2(list.drop(p1.size).take(p2.size))(Ident(JsFn.defaultName)),
            f3(list.drop(p1.size + p2.size))(Ident(JsFn.defaultName))))
          },
            p1.map(There(0, _)) ++ p2.map(There(1, _)) ++ p3.map(There(2, _)))
      }

    def makeSimpleCall(func: String, args: List[JsCore]): JsCore =
      Call(ident(func), args)

    def makeSimpleBinop(op: BinaryOperator, a1: Output, a2: Output): Output =
      Arity2(a1, a2)(BinOp(op, _, _))

    def makeSimpleUnop(op: UnaryOperator, a1: Output): Output =
      Arity1(a1)(UnOp(op, _))

    import MapFuncs._

    {
      case Nullary(v1) => v1.cata(Data.fromEJson).toJs.map[PartialJs](js => ({ case Nil => JsFn.const(js) }, Nil)) \/> NonRepresentableEJson(v1.shows)

      case Length(a1) =>
        Arity1(a1)(expr => Call(ident("NumberLong"), List(Select(expr, "length"))))

      case Date(a1) => Arity1(a1)(str =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + dateRegex + "$")))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined")))
      case Time(a1) => Arity1(a1)(str =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + timeRegex + "$")))), "test"), List(str)),
          str,
          ident("undefined")))
      case Timestamp(a1) => Arity1(a1)(str =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + timestampRegex + "$")))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined")))
      case Interval(a1) => ???
      case TimeOfDay(a1) => {
        def pad2(x: JsCore) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
              BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
              ident("x")))
        def pad3(x: JsCore) =
          Let(Name("x"), x,
            If(
              BinOp(jscore.Lt, ident("x"), Literal(Js.Num(100, false))),
              BinOp(jscore.Add, Literal(Js.Str("00")), ident("x")),
              If(
                BinOp(jscore.Lt, ident("x"), Literal(Js.Num(10, false))),
                BinOp(jscore.Add, Literal(Js.Str("0")), ident("x")),
                ident("x"))))
        Arity1(a1)(date =>
          Let(Name("t"), date,
            binop(jscore.Add,
              pad2(Call(Select(ident("t"), "getUTCHours"), Nil)),
              Literal(Js.Str(":")),
              pad2(Call(Select(ident("t"), "getUTCMinutes"), Nil)),
              Literal(Js.Str(":")),
              pad2(Call(Select(ident("t"), "getUTCSeconds"), Nil)),
              Literal(Js.Str(".")),
              pad3(Call(Select(ident("t"), "getUTCMilliseconds"), Nil)))))
      }
      case ToTimestamp(a1) => ???
      case Extract(a1, a2) =>
        // FIXME: Handle non-constant strings as well
        (HasStr(a1) ⊛ HasJs(a2)) {
          case (field, (sel, inputs)) => ((field match {
            case "century"      => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(100, false))))
            case "day"          => \/-(x => Call(Select(x, "getDate"), Nil)) // (day of month)
            case "decade"       => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(10, false))))
            // Note: MongoDB's Date's getDay (during filtering at least) seems to be monday=0 ... sunday=6,
            // apparently in violation of the JavaScript convention.
            case "dow"          =>
              \/-(x => If(BinOp(jscore.Eq,
                Call(Select(x, "getDay"), Nil),
                Literal(Js.Num(6, false))),
                Literal(Js.Num(0, false)),
                BinOp(jscore.Add,
                  Call(Select(x, "getDay"), Nil),
                  Literal(Js.Num(1, false)))))
            // TODO: case "doy"          => \/- (???)
            // TODO: epoch
            case "hour"         => \/-(x => Call(Select(x, "getHours"), Nil))
            case "isodow"       =>
              \/-(x => BinOp(jscore.Add,
                Call(Select(x, "getDay"), Nil),
                Literal(Js.Num(1, false))))
                // TODO: isoyear
            case "microseconds" =>
              \/-(x => BinOp(Mult,
                BinOp(jscore.Add,
                  Call(Select(x, "getMilliseconds"), Nil),
                  BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))),
                Literal(Js.Num(1000, false))))
            case "millennium"   => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(1000, false))))
            case "milliseconds" =>
              \/-(x => BinOp(jscore.Add,
                Call(Select(x, "getMilliseconds"), Nil),
                BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))))
            case "minute"       => \/-(x => Call(Select(x, "getMinutes"), Nil))
            case "month"        =>
              \/-(x => BinOp(jscore.Add,
                Call(Select(x, "getMonth"), Nil),
                Literal(Js.Num(1, false))))
            case "quarter"      =>
              \/-(x => BinOp(jscore.Add,
                BinOp(BitOr,
                  BinOp(Div,
                    Call(Select(x, "getMonth"), Nil),
                    Literal(Js.Num(3, false))),
                  Literal(Js.Num(0, false))),
                Literal(Js.Num(1, false))))
            case "second"       => \/-(x => Call(Select(x, "getSeconds"), Nil))
                // TODO: timezone, timezone_hour, timezone_minute
                // case "week"         => \/- (???)
            case "year"         => \/-(x => Call(Select(x, "getFullYear"), Nil))

            case _ => -\/(FuncApply("extract", "valid time period", field))
          }): PlannerError \/ (JsCore => JsCore)).map(x =>
            ({ case (list: List[JsFn]) => JsFn(JsFn.defaultName, x(sel(list)(Ident(JsFn.defaultName)))) },
              inputs.map(There(1, _))): PartialJs)
        }.join

      case Negate(a1)       => makeSimpleUnop(Neg, a1)
      case Add(a1, a2)      => makeSimpleBinop(jscore.Add, a1, a2)
      case Multiply(a1, a2) => makeSimpleBinop(Mult, a1, a2)
      case Subtract(a1, a2) => makeSimpleBinop(Sub, a1, a2)
      case Divide(a1, a2)   => makeSimpleBinop(Div, a1, a2)
      case Modulo(a1, a2)   => makeSimpleBinop(Mod, a1, a2)
      case Power(a1, a2)    => Arity2(a1, a2)((b, e) =>
        Call(Select(ident("Math"), "pow"), List(b, e)))

      case Not(a1)     => makeSimpleUnop(jscore.Not, a1)
      case Eq(a1, a2)  => makeSimpleBinop(jscore.Eq, a1, a2)
      case Neq(a1, a2) => makeSimpleBinop(jscore.Neq, a1, a2)
      case Lt(a1, a2)  => makeSimpleBinop(jscore.Lt, a1, a2)
      case Lte(a1, a2) => makeSimpleBinop(jscore.Lte, a1, a2)
      case Gt(a1, a2)  => makeSimpleBinop(jscore.Gt, a1, a2)
      case Gte(a1, a2) => makeSimpleBinop(jscore.Gte, a1, a2)
      case IfUndefined(a1, a2) => Arity2(a1, a2)((value, fallback) =>
        // TODO: Only evaluate `value` once.
        If(BinOp(jscore.Eq, value, ident("undefined")), fallback, value))
      case And(a1, a2) => makeSimpleBinop(jscore.And, a1, a2)
      case Or(a1, a2)  => makeSimpleBinop(jscore.Or, a1, a2)
      case Coalesce(a1, a2) => ???
      case Between(a1, a2, a3) => Arity3(a1, a2, a3)((value, min, max) =>
        makeSimpleCall(
          "&&",
          List(
            makeSimpleCall("<=", List(min, value)),
            makeSimpleCall("<=", List(value, max)))))
      case Cond(a1, a2, a3) => Arity3(a1, a2, a3)(If(_, _, _))

      case Within(a1, a2) => Arity2(a1, a2)((value, array) =>
        BinOp(jscore.Neq,
          Literal(Js.Num(-1, false)),
          Call(Select(array, "indexOf"), List(value))))

      case Lower(a1) => Arity1(a1)(str => Call(Select(str, "toLowerCase"), Nil))
      case Upper(a1) => Arity1(a1)(str => Call(Select(str, "toLUpperCase"), Nil))
      case Bool(a1) => Arity1(a1)(str =>
        If(BinOp(jscore.Eq, str, Literal(Js.Str("true"))),
          Literal(Js.Bool(true)),
          If(BinOp(jscore.Eq, str, Literal(Js.Str("false"))),
            Literal(Js.Bool(false)),
            ident("undefined"))))
      case Integer(a1) => Arity1(a1)(str =>
        If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + intRegex + "$")))), "test"), List(str)),
          Call(ident("NumberLong"), List(str)),
          ident("undefined")))
      case Decimal(a1) =>
        Arity1(a1)(str =>
          If(Call(Select(Call(ident("RegExp"), List(Literal(Js.Str("^" + floatRegex + "$")))), "test"), List(str)),
            Call(ident("parseFloat"), List(str)),
            ident("undefined")))
      case Null(a1) => Arity1(a1)(str =>
        If(BinOp(jscore.Eq, str, Literal(Js.Str("null"))),
          Literal(Js.Null),
          ident("undefined")))
      case ToString(a1) => Arity1(a1)(value =>
        If(isInt(value),
          // NB: This is a terrible way to turn an int into a string, but the
          //     only one that doesn’t involve converting to a decimal and
          //     losing precision.
          Call(Select(Call(ident("String"), List(value)), "replace"), List(
            Call(ident("RegExp"), List(
              Literal(Js.Str("[^-0-9]+")),
              Literal(Js.Str("g")))),
            Literal(Js.Str("")))),
          If(binop(jscore.Or, isTimestamp(value), isDate(value)),
            Call(Select(value, "toISOString"), Nil),
            Call(ident("String"), List(value)))))
      case Search(a1, a2, a3) => Arity3(a1, a2, a3)((field, pattern, insen) =>
        Call(
          Select(
            New(Name("RegExp"), List(
              pattern,
              If(insen, Literal(Js.Str("im")), Literal(Js.Str("m"))))),
            "test"),
          List(field)))
      case Substring(a1, a2, a3) => Arity3(a1, a2, a3)((field, start, len) =>
        Call(Select(field, "substr"), List(start, len)))
      // case ToId(a1) => Arity1(a1)(id => Call(ident("ObjectId"), List(id)))

      case MakeArray(a1) => ???
      case MakeMap(a1, a2) => ???
      case ConcatArrays(a1, a2) => makeSimpleBinop(jscore.Add, a1, a2)
      case ConcatMaps(a1, a2) => ???
      case ProjectField(a1, a2) => Arity2(a1, a2)(Access(_, _))
      case ProjectIndex(a1, a2)  => Arity2(a1, a2)(Access(_, _))
      case DeleteField(a1, a2)  => ???

      case Guard(expr, typ, cont, fallback) =>
        val jsCheck: Type => Option[JsCore => JsCore] =
          generateTypeCheck[JsCore, JsCore](BinOp(jscore.Or, _, _)) {
            case Type.Null             => isNull
            case Type.Dec              => isDec
            case Type.Int
               | Type.Int ⨿ Type.Dec
               | Type.Int ⨿ Type.Dec ⨿ Type.Interval
                => isAnyNumber
            case Type.Str              => isString
            case Type.Obj(_, _) ⨿ Type.FlexArr(_, _, _)
                => isObjectOrArray
            case Type.Obj(_, _)        => isObject
            case Type.FlexArr(_, _, _) => isArray
            case Type.Binary           => isBinary
            case Type.Id               => isObjectId
            case Type.Bool             => isBoolean
            case Type.Date             => isDate
          }
        jsCheck(typ).fold[OutputM[PartialJs]](
          -\/(InternalError("uncheckable type")))(
          f =>
          (HasJs(expr) ⊛ HasJs(cont) ⊛ HasJs(fallback)) {
            case ((f1, p1), (f2, p2), (f3, p3)) =>
              ({ case list => JsFn(JsFn.defaultName,
                If(f(f1(list.take(p1.size))(Ident(JsFn.defaultName))),
                  f2(list.drop(p1.size).take(p2.size))(Ident(JsFn.defaultName)),
                  f3(list.drop(p1.size + p2.size))(Ident(JsFn.defaultName))))
              },
                p1.map(There(0, _)) ++ p2.map(There(1, _)) ++ p3.map(There(2, _)))
          })

      case DupArrayIndices(_) => ???
      case DupMapKeys(_)      => ???
      case Range(_, _)        => ???
      case ZipArrayIndices(_) => ???
      case ZipMapKeys(_)      => ???
    }
  }

  trait Planner[F[_]] {
    type IT[G[_]]

    def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
      joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
      implicit I: WorkflowOpCoreF :<: WF,
               ev: Show[WorkflowBuilder[WF]],
               WB: WorkflowBuilder.Ops[WF]):
        AlgebraM[StateT[OutputM, NameGen, ?], F, WorkflowBuilder[WF]]
  }
  object Planner {
    type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

  // NB: Shouldn’t need this once we convert to paths.
  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
    new Planner[Const[DeadEnd, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        // NB: This is just a dummy value. Should never be referenced.
        κ(StateT.stateT(ValueBuilder(Bson.Null)))
    }

  implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] =
    new Planner[Const[Read, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        qs => Collection.fromFile(qs.getConst.path).bimap(PlanPathError(_): PlannerError, WB.read).liftM[GenT]
  }

  implicit def sourcedPathable[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, SourcedPathable[T, ?]] =
    new Planner[SourcedPathable[T, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) = {
        case LeftShift(src, struct, repair) => ???
        // (src ⊛ getJsFn(struct) ⊛ getJsMerge(repair))(
        //   (wb, js, jm) =>
        //   WB.jsExpr(
        //     List(wb, WB.flattenMap(WB.jsExpr1(wb, js))),
        //     jm))
        case Union(src, lBranch, rBranch) => ???
      }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) = {
        case qscript.Map(src, f) =>
          getJsFn(f).map(WB.jsExpr1(src, _)).liftM[GenT]
        case Reduce(src, bucket, reducers, repair) =>
          (getJsFn(bucket) ⊛
            reducers.traverse(_.traverse(getExpr[T])) ⊛
            getJsRed(repair))((b, red, rep) =>
            ExprBuilder(
              GroupBuilder(src,
                List(ExprBuilder(src, b.left)),
                Contents.Doc(red.zipWithIndex.map(ai =>
                  (BsonField.Name(ai._2.toString),
                    accumulator(ai._1).left[Expression])).toListMap)),
              rep.left)).liftM[GenT]
        case Sort(src, bucket, order) =>
          val (keys, dirs) = ((bucket, SortDir.Ascending) :: order).unzip
          keys.traverse(getJsFn[T]).map(ks =>
            WB.sortBy(src, ks.map(WB.jsExpr1(src, _)), dirs)).liftM[GenT]
        case Filter(src, f) =>
          getJsFn(f).map(js =>
            WB.filter(src, List(WB.jsExpr1(src, js)), {
              case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Bool(true)))
            })).liftM[GenT]
        case Take(src, from, count) =>
          (rebaseWB(joinHandler, from, src) ⊛
            (rebaseWB(joinHandler, count, src) >>= (HasInt(_).liftM[GenT])))(
            WB.limit)
        case Drop(src, from, count) =>
          (rebaseWB(joinHandler, from, src) ⊛
            (rebaseWB(joinHandler, count, src) >>= (HasInt(_).liftM[GenT])))(
            WB.skip)
      }
    }

  implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        qs =>
          // FIXME: we should take advantage of the already merged srcs
          (rebaseWB(joinHandler, qs.lBranch, qs.src) ⊛
            rebaseWB(joinHandler, qs.rBranch, qs.src) ⊛
            getExpr(qs.lKey).map(e => ExprBuilder(qs.src, e.right)).liftM[GenT] ⊛
            getExpr(qs.rKey).map(e => ExprBuilder(qs.src, e.right)).liftM[GenT])(
            (lb, rb, lk, rk) =>
            joinHandler.run(
              qs.f match {
                case Inner => set.InnerJoin
                case FullOuter => set.FullOuterJoin
                case LeftOuter => set.LeftOuterJoin
                case RightOuter => set.RightOuterJoin
              },
              JoinSource(lb, List(lk), getJsFn(qs.lKey).toOption.map(List(_))),
              JoinSource(rb, List(rk), getJsFn(qs.rKey).toOption.map(List(_))))).join

    }

  // TODO: Remove this instance
  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
    new Planner[ThetaJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        ???
    }

  // TODO: Remove this instance
  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
    new Planner[ProjectBucket[T, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        ???
    }

  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
      Planner.Aux[T, Coproduct[F, G, ?]] =
    new Planner [Coproduct[F, G, ?]] {
      type IT[G[_]] = T[G]
      def plan[WF[_]: Functor: Coalesce: Crush: Crystallize](
        joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
        implicit I: WorkflowOpCoreF :<: WF,
                 ev: Show[WorkflowBuilder[WF]],
                 WB: WorkflowBuilder.Ops[WF]) =
        _.run.fold(F.plan(joinHandler), G.plan(joinHandler))
    }
  }

  def getExpr[T[_[_]]: Recursive: ShowT](fm: FreeMap[T]): OutputM[Expression] =
    processMapFuncExpr(fm)(κ($field("value").right))

  def getJsFn[T[_[_]]: Recursive: ShowT](fm: FreeMap[T]): OutputM[JsFn] =
    processMapFunc(fm)(κ(JsFn.identity))

  def getJsMerge[T[_[_]]: Recursive: ShowT](jf: JoinFunc[T]):
      List[JsCore] => OutputM[JsFn] =
    l => processMapFunc(jf) {
      case LeftSide => JsFn(JsFn.defaultName, l(0))
      case RightSide => JsFn(JsFn.defaultName, l(1))
    }

  def getJsRed[T[_[_]]: Recursive: ShowT](jr: Free[MapFunc[T, ?], ReduceIndex]):
      OutputM[JsFn] =
    processMapFunc(jr)(ri => JsFn.const(jscore.ident(ri.idx.toString)))

  def rebaseWB[T[_[_]], WF[_]: Functor: Coalesce: Crush: Crystallize](
    joinHandler: JoinHandler[WF, WorkflowBuilder.M], free: FreeQS[T], src: WorkflowBuilder[WF])(
    implicit F: Planner.Aux[T, QScriptTotal[T, ?]],
             I: WorkflowOpCoreF :<: WF,
             ev: Show[WorkflowBuilder[WF]],
             WB: WorkflowBuilder.Ops[WF]):
      StateT[OutputM, NameGen, WorkflowBuilder[WF]] =
    freeCataM(free)(
      interpretM[StateT[OutputM, NameGen, ?], QScriptTotal[T, ?], qscript.Hole, WorkflowBuilder[WF]](κ(StateT.stateT(src)), F.plan(joinHandler)))

  def HasLiteral[WF[_]]: WorkflowBuilder[WF] => OutputM[Bson] =
    wb => asLiteral(wb) \/> FuncApply("", "literal", wb.toString)

  def HasInt[WF[_]]: WorkflowBuilder[WF] => OutputM[Long] = HasLiteral(_) >>= {
    case Bson.Int32(v) => \/-(v.toLong)
    case Bson.Int64(v) => \/-(v)
    case x => -\/(FuncApply("", "64-bit integer", x.toString))
  }

  // This is maybe worth putting in Matryoshka?
  def findFirst[T[_[_]]: Recursive, F[_]: Functor: Foldable, A](
    f: PartialFunction[T[F], A]):
      CoalgebraM[A \/ ?, F, T[F]] =
    tf => (f.lift(tf) \/> tf.project).swap

  object Roll {
    def unapply[S[_]: Functor, A](obj: Free[S, A]): Option[S[Free[S, A]]] =
      obj.resume.swap.toOption
  }

  object Point {
    def unapply[S[_]: Functor, A](obj: Free[S, A]): Option[A] = obj.resume.toOption
  }

  def elideMoreGeneralGuards[T[_[_]]](subType: Type):
      FreeMap[T] => PlannerError \/ FreeMap[T] = {
    case free @ Roll(MapFuncs.Guard(Point(SrcHole), typ, cont, _)) =>
      if (typ.contains(subType)) cont.right
      else if (!subType.contains(typ))
        InternalError("can only contain " + subType + ", but a(n) " + typ + " is expected").left
      else free.right
    case x => x.right
  }

  // TODO: Allow backends to provide a “Read” type to the typechecker, which
  //       represents the type of values that can be stored in a collection.
  //       E.g., for MongoDB, it would be `Map(String, Top)`. This will help us
  //       generate more correct PatternGuards in the first place, rather than
  //       trying to strip out unnecessary ones after the fact
  def assumeReadType[T[_[_]]: Recursive, F[_]: Functor](typ: Type)(
    implicit QC: QScriptCore[T, ?] :<: F, R: Const[Read, ?] :<: F):
      QScriptCore[T, T[F]] => PlannerError \/ F[T[F]] = {
    case m @ qscript.Map(src, mf) =>
      R.prj(src.project).fold(
        QC.inj(m).right[PlannerError])(
        κ(mf.transCataTM(elideMoreGeneralGuards(typ)) ∘
          (mf => QC.inj(qscript.Map(src, mf)))))
    case qc => QC.inj(qc).right
  }

  type GenT[X[_], A]  = StateT[X, NameGen, A]

  def plan0[T[_[_]]: Recursive: Corecursive: EqualT: ShowT,
            WF[_]: Functor: Coalesce: Crush: Crystallize](
    joinHandler: JoinHandler[WF, WorkflowBuilder.M])(
    lp: T[LogicalPlan])(
    implicit I: WorkflowOpCoreF :<: WF,
             ev: Show[WorkflowBuilder[WF]],
             WB: WorkflowBuilder.Ops[WF],
             R: Delay[RenderTree, WF]):
      EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[WF]] = {
    val optimize = new Optimize[T]

    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import EitherT.eitherTMonad

    // NB: Locally add state on top of the result monad so everything
    //     can be done in a single for comprehension.
    type PlanT[X[_], A] = EitherT[X, PlannerError, A]
    type W[A]           = Writer[PhaseResults, A]
    type F[A]           = PlanT[W, A]
    type M[A]           = GenT[F, A]

    def log[A: RenderTree](label: String)(ma: M[A]): M[A] =
      ma flatMap { a =>
        val result = PhaseResult.Tree(label, RenderTree[A].render(a))
        (Writer(Vector(result), a): W[A]).liftM[PlanT].liftM[GenT]
      }

    def swizzle[A](sa: StateT[PlannerError \/ ?, NameGen, A]): M[A] =
      StateT[F, NameGen, A](ng => EitherT(sa.run(ng).point[W]))

    def liftError[A](ea: PlannerError \/ A): M[A] =
      EitherT(ea.point[W]).liftM[GenT]

    val P = scala.Predef.implicitly[Planner.Aux[T, QScriptTotal[T, ?]]]

    (for {
      qs  <- QueryFile.convertToQScript(lp).liftM[StateT[?[_], NameGen, ?]]
      // TODO: also need to prefer projections over deletions
      // NB: right now this only outputs one phase, but it’d be cool if we could
      //     interleave phase building in the composed recursion scheme
      opt <- log("QScript (Mongo-specific)")(liftError(
        qs.transCataM[PlannerError \/ ?, QScriptTotal[T, ?]](tf =>
          (liftFGM(assumeReadType[T, QScriptTotal[T, ?]](Type.Obj(ListMap(), Some(Type.Top)))) // ⋘ optimize.simplifyJoins
          ).apply(tf) ∘
            Normalizable[QScriptTotal[T, ?]].normalize)))
      wb  <- log("Workflow Builder")(swizzle(opt.cataM[StateT[OutputM, NameGen, ?], WorkflowBuilder[WF]](P.plan(joinHandler) ∘ (_ ∘ (_ ∘ normalize)))))
      wf1  <- log("Workflow (raw)")         (swizzle(WorkflowBuilder.build(wb)))
      wf2  <- log("Workflow (crystallized)")(Crystallize[WF].crystallize(wf1).point[M])
    } yield wf2).evalZero
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context (which is for the time being just the "query model"
    * associated with the backend version.)
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    */
  def plan[T[_[_]]: Recursive: Corecursive: EqualT: ShowT](
    logical: T[LogicalPlan], queryContext: fs.QueryContext):
      EitherT[Writer[PhaseResults, ?], PlannerError, Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    queryContext.model match {
      case `3.2` =>
        val joinHandler =
          JoinHandler.fallback(
            JoinHandler.pipeline[Workflow3_2F](queryContext.statistics),
            JoinHandler.mapReduce[Workflow3_2F])
        plan0[T, Workflow3_2F](joinHandler)(logical)

      case _     =>
        val joinHandler = JoinHandler.mapReduce[Workflow2_6F]
        plan0[T, Workflow2_6F](joinHandler)(logical).map(_.inject[WorkflowF])
    }
  }
}

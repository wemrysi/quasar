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

package quasar.compile

import slamdata.Predef._
import quasar.common.CIName
import quasar.contrib.pathy._
import quasar.contrib.std._
import quasar.fp.ski._
import quasar.sql._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

object ResolveImports {
  def apply[T[_[_]]: BirecursiveT, M[_]: Monad: MonadSemanticErr](
      scopedExpr: ScopedExpr[T[Sql]],
      baseDir: ADir,
      retrieve: ADir => M[List[Statement[T[Sql]]]])
      : M[T[Sql]] = {

    def absImport(i: Import[T[Sql]], from: ADir): SemanticError \/ ADir =
      refineTypeAbs(i.path).fold(sandboxCurrent(_), r => sandboxCurrent(unsandbox(from) </> r))
        .toRightDisjunction {
          val invalidPathString = posixCodec.unsafePrintPath(i.path)
          val fromString = posixCodec.printPath(from)
          SemanticError.genericError(s"$invalidPathString is invalid because it is located at $fromString")
        }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def scopeFromHere(
        imports: List[Import[T[Sql]]],
        funcsHere: List[FunctionDecl[T[Sql]]],
        here: ADir)
        : (CIName, Int) => M[List[(FunctionDecl[T[Sql]], ADir)]] = {
      case (name, arity) =>
        imports.traverse(absImport(_, here)).fold(
          MonadSemanticErr[M].raiseError(_),
          absImportPaths => {
            // All functions coming from `imports` along with their respective import statements and where they are defined
            val funcsFromImports = absImportPaths.traverse(d => retrieve(d).map(stats => (stats.decls, stats.imports, d)))
            // All functions in "this" scope along with their own imports
            val allFuncs = funcsFromImports.map((funcsHere, imports, here) :: _)
            allFuncs.flatMap(_.traverse { case (funcs, imports, from) =>
              def matchesSignature(func: FunctionDecl[T[Sql]]) = func.name === name && arity === func.args.size
              funcs.filter(matchesSignature).traverse { decl =>
                val others = funcs.filterNot(matchesSignature) // No recursice calls in SQL^2 so we don't include ourselves
                val currentScope = scopeFromHere(imports, others, from)
                decl.traverse(q => inlineInvokes(q.mkPathsAbsolute(from), currentScope)).strengthR(from)
              }
            }).map(_.join)
          })
    }

    inlineInvokes(scopedExpr.expr, scopeFromHere(scopedExpr.imports, scopedExpr.defs, baseDir))
  }

  def applyFunction[T[_[_]]: BirecursiveT](
      func: FunctionDecl[T[Sql]],
      args: List[T[Sql]])
      : SemanticError \/ T[Sql] = {

    val expected = func.args.size
    val actual   = args.size

    func.args.duplicates.headOption match {
      case Some(dupes) =>
        SemanticError.InvalidFunctionDefinition(
          func.map(_.convertTo[Fix[Sql]]),
          s"parameter :${dupes.head.value} is defined multiple times").left

      case None =>
        if (expected ≠ actual) {
          SemanticError.WrongArgumentCount(func.name, expected, actual).left
        } else {
          val argMap = func.args.zip(args).toMap
          func.body.cataM[SemanticError \/ ?, T[Sql]] {
            case v: Vari[T[Sql]] =>
              // Leave the variable there in case it will be substituted by an external variable
              argMap.getOrElse(CIName(v.symbol), v.embed).right

            case s: Select[T[Sql]] =>
              substituteRelationVariable(s) { v =>
                argMap.getOrElse(CIName(v.symbol), v.embed)
              } map (_.embed)

            case other => other.embed.right
          }
        }
      }
  }

  /**
    * Inlines all function invocations with the bodies of functions in scope.
    * Leaves invocations to functions outside of scope untouched (as opposed to erroring out)
    * @param scope Returns the list of function definitions that match a given name and function arity along with a
    *              path specifying whether this function was found
    */
  def inlineInvokes[T[_[_]]: BirecursiveT, M[_]: Monad: MonadSemanticErr](
      q: T[Sql],
      scope: (CIName, Int) => M[List[(FunctionDecl[T[Sql]], ADir)]])
      : M[T[Sql]] = {
    q.cataM[M, T[Sql]] {
      case invoke @ InvokeFunction(name, args) =>
        scope(name, args.size) flatMap {
          case Nil =>
            invoke.embed.point[M]

          case List((funcDef, _)) =>
            MonadSemanticErr[M].unattempt(applyFunction(funcDef, args).point[M])

          case ambiguous =>
            MonadSemanticErr[M].raiseError(
              SemanticError.ambiguousFunctionInvoke(name, ambiguous.map(_.leftMap(_.name))))
        }

      case other =>
       other.embed.point[M]
    }
  }

  def substituteRelationVariable[T](
      select: Select[T])(
      mapping: Vari[T] => T)(
      implicit
      T0: Recursive.Aux[T, Sql],
      T1: Corecursive.Aux[T, Sql])
      : SemanticError \/ Select[T] = {

    val newRelation = select.relation.traverse(_.transformM[SemanticError \/ ?, T]({
      case VariRelationAST(vari, alias) =>
        mapping(vari).project match {
          case Ident(name) =>
            posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(name)
              .map(TableRelationAST(_, alias): SqlRelation[T])
              .toRightDisjunction(SemanticError.genericError(
                s"bad path: $name (note: absolute file path required)")) // FIXME

          // If the variable points to another variable, substitute the old one for the new one
          case Vari(symbol) =>
            VariRelationAST(Vari(symbol), alias).right

          case x =>
            SemanticError.genericError(s"not a valid table name: ${pprint(x.embed)}").left // FIXME
        }

      case otherRelation => otherRelation.right[SemanticError]
    }, _.right[SemanticError]))

    newRelation.map(r => select.copy(relation = r))
  }
}

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

package quasar

import slamdata.Predef.{List, Option, String, Vector}
import quasar.common.{phase, PhaseResult, PhaseResultW}
import quasar.contrib.scalaz.MonadError_
import quasar.sql._

import scalaz.{\/, EitherT, Monad, NonEmptyList}

package object compile {
  type SemanticErrors = NonEmptyList[SemanticError]

  type MonadSemanticErrs[F[_]] = MonadError_[F, SemanticErrors]

  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  def addOffsetLimit[T]
      (lp: T, off: Natural, lim: Option[Positive])
      (implicit T: Corecursive.Aux[T, LP])
      : T = {
    val skipped = Drop(lp, constant[T](Data.Int(off.value)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, constant[T](Data.Int(l.value)).embed).embed)
  }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  def precompile[F[_], T: Equal: RenderTree: Show]
      (query: Fix[Sql], vars: Variables, basePath: ADir)
      (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : F[T] = {
    import SemanticAnalysis._
    for {
      ast      <- phase[F]("SQL AST", query.right)
      substAst <- phase("Variables Substituted", Variables.substVars(ast, vars))
      absAst   <- phase("Absolutized", substAst.mkPathsAbsolute(basePath).right)
      sortProj <- phase("Sort Keys Projected", projectSortKeys[Fix[Sql]](absAst).right)
      annAst   <- phase("Annotated Tree", annotate[Fix[Sql]](sortProj))
      logical  <- phase("Logical Plan", Compiler.compile[T](annAst) leftMap (_.wrapNel))
    } yield logical
  }

  /** Returns the name of the expression when viewed as a projection
    * (of the optional relation), if available.
    */
  def projectionName[T](
    expr: T,
    relationName: Option[String]
  )(implicit
    T: Recursive.Aux[T, Sql]
  ): Option[String] = {
    val loop: T => (Option[String] \/ (Option[String] \/ T)) =
      _.project match {
        case Ident(name) if !relationName.element(name)  => some(name).left
        case Binop(_, Embed(StringLiteral(v)), KeyDeref) => some(v).left
        case Unop(arg, FlattenMapValues)                 => arg.right.right
        case Unop(arg, FlattenArrayValues)               => arg.right.right
        case _                                           => None.left
      }

    \/.loopRight(expr.right, ι[Option[String]], loop)
  }

  def projectionNames[T]
    (projections: List[Proj[T]], relName: Option[String])
    (implicit T: Recursive.Aux[T, Sql])
      : SemanticError \/ List[(String, T)] = {
    val aliases = projections.flatMap(_.alias.toList)

    (aliases diff aliases.distinct).headOption.cata[SemanticError \/ List[(String, T)]](
      duplicateAlias => SemanticError.DuplicateAlias(duplicateAlias).left,
      projections.zipWithIndex.mapAccumLeftM(aliases.toSet) { case (used, (Proj(expr, alias), index)) =>
        alias.cata(
          a => (used, a -> expr).right,
          {
            val tentativeName = projectionName(expr, relName) getOrElse index.toString
            val alternatives = Stream.from(0).map(suffix => tentativeName + suffix.toString)
            (tentativeName #:: alternatives).dropWhile(used.contains).headOption.map { name =>
              // WartRemover seems to be confused by the `+` method on `Set`
              @SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
              val newUsed = used + name
              (newUsed, name -> expr)
            } \/> SemanticError.GenericError("Could not generate alias for a relation") // unlikely since we know it's an quasi-infinite stream
          })
      }.map(_._2))
  }

  /** Returns the `LogicalPlan` for the given SQL^2 query */
  def queryPlan(
    expr: Fix[Sql], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive]):
      CompileM[Fix[LP]] =
    precompile[Fix[LP]](expr, vars, basePath)
      .flatMap(lp => preparePlan(addOffsetLimit(lp, off, lim)))

  def resolveImports[S[_]](scopedExpr: ScopedExpr[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S],
    fsFail: Failure.Ops[FileSystemError, S]
  ): EitherT[Free[S, ?], SemanticError, Fix[Sql]] =
    EitherT(fsFail.unattemptT(resolveImports_(scopedExpr, baseDir).run))

  def resolveImports_[S[_]](scopedExpr: ScopedExpr[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S]
  ): EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, Fix[Sql]] =
    resolveImportsImpl[EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, ?], Fix](
      scopedExpr,
      baseDir,
      d => EitherT(EitherT(mount
             .lookupModuleConfig(d)
             .bimap(e => SemanticError.genericError(e.shows), _.statements)
             .run.run ∘ (_ \/> (pathErr(pathNotFound(d))))))
    ).run >>= (i => EitherT(EitherT.rightT(i.η[Free[S, ?]])))
}

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

import slamdata.Predef._
import quasar.common.{phase, PhaseResult, PhaseResultW, PhaseResultTell}
import quasar.contrib.pathy.ADir
import quasar.contrib.scalaz.MonadError_
import quasar.fp.numeric.{Natural, Positive}
import quasar.fp.ski._
import quasar.fs.mount.Mounting
import quasar.frontend.logicalplan.{constant, LogicalPlan => LP}
import quasar.sql._
import quasar.std.SetLib.{Drop, Take}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

package object compile {
  type SemanticErrors = NonEmptyList[SemanticError]

  type MonadSemanticErr[F[_]] = MonadError_[F, SemanticError]

  def MonadSemanticErr[F[_]](implicit ev: MonadSemanticErr[F]): MonadSemanticErr[F] = ev

  type MonadSemanticErrs[F[_]] = MonadError_[F, SemanticErrors]

  def MonadSemanticErrs[F[_]](implicit ev: MonadSemanticErrs[F]): MonadSemanticErrs[F] = ev

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

  def allVariables: Algebra[Sql, List[VarName]] = {
    case Vari(name) => List(VarName(name))

    case sel @ Select(_, _, rel, _, _, _) =>
      rel.toList.collect {
        case VariRelationAST(vari, _) => VarName(vari.symbol)
      } ++ (sel: Sql[List[VarName]]).fold

    case other => other.fold
  }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  def precompile[F[_]: Monad: PhaseResultTell, T: Equal: RenderTree: Show]
      (query: Fix[Sql], vars: Variables, basePath: ADir)
      (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : F[T] = {
    import SemanticAnalysis._
    for {
      ast      <- phase[F]("SQL AST", query)
      substAst <- phase[F]("Variables Substituted", Variables.substVars(ast, vars))
      absAst   <- phase[F]("Absolutized", substAst.mkPathsAbsolute(basePath).right)
      sortProj <- phase[F]("Sort Keys Projected", projectSortKeys[Fix[Sql]](absAst).right)
      annAst   <- phase[F]("Annotated Tree", annotate[Fix[Sql]](sortProj))
      logical  <- phase[F]("Logical Plan", Compiler.compile[T](annAst) leftMap (_.wrapNel))
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
    ResolveImports[Fix, EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, ?]](
      scopedExpr,
      baseDir,
      d => EitherT(EitherT(mount
             .lookupModuleConfig(d)
             .bimap(e => SemanticError.genericError(e.shows), _.statements)
             .run.run ∘ (_ \/> (pathErr(pathNotFound(d))))))
    ).run >>= (i => EitherT(EitherT.rightT(i.η[Free[S, ?]])))

  def substVarsƒ(vars: Variables): AlgebraM[SemanticError \/ ?, Sql, Fix[Sql]] = {
    case Vari(name) =>
      vars.lookup(VarName(name))

    case sel: Select[Fix[Sql]] =>
      sel.substituteRelationVariable[SemanticError \/ ?, Fix[Sql]](v =>
        vars.lookup(VarName(v.symbol))).join.map(_.embed)

    case x => x.embed.right
  }

  def substVars(expr: Fix[Sql], variables: Variables): SemanticErrors \/ Fix[Sql] = {
    val allVars = expr.cata(allVariables)
    val errors = allVars.map(variables.lookup(_)).collect { case -\/(semErr) => semErr }.toNel
    errors.fold(
      expr.cataM[SemanticError \/ ?, Fix[Sql]](substVarsƒ(variables)).leftMap(_.wrapNel))(
      errors => errors.left)
  }

  /*
  value.get(name).fold[SemanticError \/ Fix[Sql]](
    UnboundVariable(name).left)(
    varValue => fixParser.parseExpr(varValue.value)
      .leftMap(VariableParseError(name, varValue, _)))
  */
}

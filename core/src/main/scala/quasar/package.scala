/*
 * Copyright 2014–2017 SlamData Inc.
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

import slamdata.Predef._
import quasar.common.{PhaseResult, PhaseResultW}
import quasar.connector.CompileM
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.frontend.{SemanticErrors, SemanticErrsT}
import quasar.frontend.logicalplan.{LogicalPlan => LP, Free => _, _}
import quasar.fs.{FileSystemError, FileSystemErrT}
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.fs.mount.{Mounting, MountConfig}
import quasar.sql._
import quasar.std.StdLib.set._

// Needed for unzip
import scala.Predef.{Map => _, _}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

package object quasar {
  private def phase[A: RenderTree](label: String, r: SemanticErrors \/ A):
      CompileM[A] =
    EitherT(r.point[PhaseResultW]) flatMap { a =>
      (a.set(Vector(PhaseResult.tree(label, a)))).liftM[SemanticErrsT]
    }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  // TODO: Move this into the SQL package, provide a type class for it in core.
  def precompile[T: Equal: RenderTree]
    (query: Block[Fix[Sql]], vars: Variables, basePath: ADir)
    (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
      : CompileM[T] = {
    import SemanticAnalysis._
    for {
      ast      <- phase("SQL AST", query.right)
      substAst <- phase("Variables Substituted", ast.mapExpressionM(Variables.substVars(_, vars)))
      absAst   <- phase("Absolutized", substAst.map(_.mkPathsAbsolute(basePath)).right)
      normed   <- phase("Normalized Projections", absAst.map(normalizeProjections[Fix[Sql]]).right)
      sortProj <- phase("Sort Keys Projected", normed.map(projectSortKeys[Fix[Sql]]).right)
      annBlock <- phase("Annotated Tree", (sortProj.traverse(annotate[Fix[Sql]])))
      logical  <- phase("Logical Plan", Compiler.compile[T](annBlock.expr, annBlock.defs) leftMap (_.wrapNel))
    } yield logical
  }

  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan.
    */
  def preparePlan(lp: Fix[LP]): CompileM[Fix[LP]] =
    for {
      optimized   <- phase("Optimized", optimizer.optimize(lp).right)
      typechecked <- phase("Typechecked", lpr.ensureCorrectTypes(optimized).disjunction)
      rewritten   <- phase("Rewritten Joins", optimizer.rewriteJoins(typechecked).right)
    } yield rewritten

  /** Identify plans which reduce to a (set of) constant value(s). */
  def refineConstantPlan(lp: Fix[LP]): List[Data] \/ Fix[LP] =
    lp.project match {
      case Constant(Data.Set(records)) => records.left
      case Constant(value)             => List(value).left
      case _                           => lp.right
    }

  def resolveImports[S[_]](blob: Blob[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S],
    fsFail: Failure.Ops[FileSystemError, S]
  ): EitherT[Free[S, ?], SemanticError, Block[Fix[Sql]]] =
    EitherT(fsFail.unattemptT(resolveImports_(blob, baseDir).run))

  def resolveImports_[S[_]](blob: Blob[Fix[Sql]], baseDir: ADir)(implicit
    mount: Mounting.Ops[S]
  ): EitherT[FileSystemErrT[Free[S, ?], ?], SemanticError, Block[Fix[Sql]]] =
    resolveImportsImpl(blob, baseDir, d => mount.lookupModuleConfig(d).toRight(pathErr(pathNotFound(d))))

  // It would be nice if this were in the sql package but that is not possible right now because
  // Mounting is defined in core
  def resolveImportsImpl[M[_]: Monad](blob: Blob[Fix[Sql]], baseDir: ADir, retrieve: ADir => M[MountConfig.ModuleConfig])
    : EitherT[M, SemanticError, Block[Fix[Sql]]] = {

    def absolutizeImport(i: Import[Fix[Sql]], from: ADir): ADir = refineTypeAbs(i.path).fold(ι, from </> _)

    def inlineInvokes(in: Fix[Sql], scope: List[ADir]): EitherT[M, SemanticError, Fix[Sql]] = {
      in.cataM[EitherT[M, SemanticError, ?], Fix[Sql]] {
        case invoke @ InvokeFunction(name, args) =>
          EitherT(findMatchingDecs(name, scope).flatMap(_ match {
            case Nil =>
              scala.Predef.println("hmm")
              invoke.embed.right.point[M] // Leave the invocation there in case it's a library function
            case List(((funcDef, newScope), _)) =>
              scala.Predef.println("sss")
              EitherT(funcDef.applyArgs(args).point[M]).flatMap(inlineInvokes(_, newScope)).run
            case ambiguous =>
              SemanticError.ambiguousImport(name, args.size, ambiguous.unzip._2.map(Import[Fix[Sql]](_))).left.point[M]
          }))
        case other => EitherT.right(other.embed.point[M])
      }
    }

    def findMatchingDecs(name: CIName, scope: List[ADir]): M[List[((FunctionDecl[Fix[Sql]], List[ADir]), ADir)]] =
      scope.traverse(d => resolveImport(d).strengthL(d)).map(maps =>
        maps.flatMap{ case(d, map) => map.get(name).toList.strengthR(d)})

    def resolveImport(at: ADir): M[Map[CIName, (FunctionDecl[Fix[Sql]], List[ADir])]] =
      retrieve(at).map { module =>
        val scope = module.imports.map(absolutizeImport(_, at))
        module.declarations.map(d => (d.name, (d, scope))).toMap
      }

    // This blob has no more imports (assuming the implementation of `inlineInvokes` is correct)
    val blobInlined = blob.traverse(inlineInvokes(_, blob.imports.map(absolutizeImport(_, baseDir))))
    blobInlined.map(blob => Block(blob.expr, blob.defs))
  }

  /** Returns the `LogicalPlan` for the given SQL^2 query, or a list of
    * results, if the query was foldable to a constant.
    */
  def queryPlan(
    block: Block[Fix[Sql]], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive]):
      CompileM[List[Data] \/ Fix[LP]] =
    precompile[Fix[LP]](block, vars, basePath)
      .flatMap(lp => preparePlan(addOffsetLimit(lp, off, lim)))
      .map(refineConstantPlan)

  def addOffsetLimit[T]
    (lp: T, off: Natural, lim: Option[Positive])
    (implicit T: Corecursive.Aux[T, LP])
      : T = {
    val skipped = Drop(lp, constant[T](Data.Int(off.value)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, constant[T](Data.Int(l.value)).embed).embed)
  }
}

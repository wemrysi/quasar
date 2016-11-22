"At this point it is probably useful to model after an existing (non-“skeleton”) filesystem."

 -- https://github.com/quasar-analytics/quasar/wiki/Adding-a-Filesystem

/** One gets the feeling each of these filesystems was started by cloning
 *  the mongo filesystem? I consider this to be outrageously high complexity
 *  to be repeating at the leaves. The expressions in which these common
 *  slugs are embedded are HUGE even without this.
 *
 *  Truly, how many different ways are people expected to consume qscript?
 *  Do they really need a fully programmable LogicalPlan->Qscript machine
 *  with a great many knobs? Shouldn't this be reduced to a configuration
 *  problem and accomplished declaratively?
 */

couchbase/.../queryfile.scala:246:                   repeatedly(C.coalesceQC[CBQScript](idPrism))     >>>
couchbase/.../queryfile.scala:247:                   repeatedly(C.coalesceEJ[CBQScript](idPrism.get)) >>>
couchbase/.../queryfile.scala:248:                   repeatedly(C.coalesceSR[CBQScript](idPrism))     >>>
couchbase/.../queryfile.scala:249:                   repeatedly(Normalizable[CBQScript].normalizeF(_: CBQScript[Fix[CBQScript]])))
marklogic/.../queryfile.scala:90:                       repeatedly(C.coalesceQC[MLQScript](idPrism)) ⋙
marklogic/.../queryfile.scala:91:                       repeatedly(C.coalesceTJ[MLQScript](idPrism.get)) ⋙
marklogic/.../queryfile.scala:92:                       repeatedly(C.coalesceSR[MLQScript](idPrism)) ⋙
marklogic/.../queryfile.scala:93:                       repeatedly(Normalizable[MLQScript].normalizeF(_: MLQScript[Fix[MLQScript]])))
mongodb/.../plannerQScript.scala:863:            repeatedly(C.coalesceQC[MongoQScript](idPrism)) ⋙
mongodb/.../plannerQScript.scala:864:            repeatedly(C.coalesceEJ[MongoQScript](idPrism.get)) ⋙
mongodb/.../plannerQScript.scala:865:            repeatedly(C.coalesceSR[MongoQScript](idPrism)) ⋙
mongodb/.../plannerQScript.scala:866:            repeatedly(Normalizable[MongoQScript].normalizeF(_: MongoQScript[T[MongoQScript]])))
sparkcore/.../queryfile.scala:81:          repeatedly(C.coalesceQC[SparkQScript](idPrism)) ⋙
sparkcore/.../queryfile.scala:82:            repeatedly(C.coalesceEJ[SparkQScript](idPrism.get)) ⋙
sparkcore/.../queryfile.scala:83:            repeatedly(C.coalesceSR[SparkQScript](idPrism)))


/** Brief excerpt from spark. How can it be needed in spark but
 *  not elsewhere?
 */

private def lt(d1: Data, d2: Data): Data = (d1, d2) match {
  case (Data.Int(a), Data.Int(b)) => Data.Bool(a < b)
  case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a < b)
  case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) < 0)
  case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) < 0)
  case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) < 0)
  case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) < 0)
  case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) < 0)
  case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
    case (false, true) => Data.Bool(true)
    case _ => Data.Bool(false)
  }
  case _ => undefined
}

private def lte(d1: Data, d2: Data): Data = (d1, d2) match {
  case (Data.Int(a), Data.Int(b)) => Data.Bool(a <= b)
  case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a <= b)
  case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) <= 0)
  case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) <= 0)
  case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) <= 0)
  case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) <= 0)
  case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) <= 0)
  case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
    case (true, false) => Data.Bool(false)
    case _ => Data.Bool(true)
  }
  case _ => undefined
}

private def gt(d1: Data, d2: Data): Data = (d1, d2) match {
  case (Data.Int(a), Data.Int(b)) => Data.Bool(a > b)
  case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a > b)
  case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) > 0)
  case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) > 0)
  case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) > 0)
  case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) > 0)
  case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) > 0)
  case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
    case (true, false) => Data.Bool(true)
    case _ => Data.Bool(false)
  }
  case _ => undefined
}


/**
 *  Some possible starting points for modeling.
 *  There is unfortunately no canonical model for almost anything,
 *  due to the full-featured backend not using qscript.
 */

/***
 * Marklogic
 ***/

def interpret[S[_]](
  resultsChunkSize: Positive
)(implicit
  S0: SessionIO :<: S,
  S1: ContentSourceIO :<: S,
  S2: Task :<: S,
  S3: MLResultHandles :<: S,
  S4: MonotonicSeq :<: S
): QueryFile ~> Free[S, ?] = {
  type QPlan[A]          = FileSystemErrT[PhaseResultT[Free[S, ?], ?], A]
  type PrologsT[F[_], A] = WriterT[F, Prologs, A]

  def liftQP[F[_], A](fa: F[A])(implicit ev: F :<: S): QPlan[A] =
    lift(fa).into[S].liftM[PhaseResultT].liftM[FileSystemErrT]

  def lpToXQuery(lp: Fix[LogicalPlan]): QPlan[MainModule] = {
    type MLQScript[A] = QScriptShiftRead[Fix, A]
    type MLPlan[A]    = PrologsT[MarkLogicPlanErrT[PhaseResultT[Free[S, ?], ?], ?], A]
    type QSR[A]       = QScriptRead[Fix, A]

    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import WriterT.writerTMonad
    val rewrite = new Rewrite[Fix]
    val C = Coalesce[Fix, MLQScript, MLQScript]

    def logPhase(pr: PhaseResult): QPlan[Unit] =
      MonadTell[QPlan, PhaseResults].tell(Vector(pr))

    def plan(qs: Fix[MLQScript]): MarkLogicPlanErrT[PhaseResultT[Free[S, ?], ?], MainModule] =
      qs.cataM(MarkLogicPlanner[MLPlan, MLQScript].plan).run map {
        case (prologs, xqy) => MainModule(Version.`1.0-ml`, prologs, xqy)
      }

    val linearize: Algebra[MLQScript, List[MLQScript[ExternallyManaged]]] =
      qsr => qsr.as[ExternallyManaged](Extern) :: Foldable[MLQScript].fold(qsr)

    for {
      qs      <- convertToQScriptRead[Fix, QPlan, QSR](d => liftQP(ops.ls(d)))(lp)
      shifted =  shiftRead[Fix](qs)
      _       <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted.cata(linearize).reverse))
      optmzed =  shifted
                   .transAna(
                     repeatedly(C.coalesceQC[MLQScript](idPrism)) ⋙
                     repeatedly(C.coalesceTJ[MLQScript](idPrism.get)) ⋙
                     repeatedly(C.coalesceSR[MLQScript](idPrism)) ⋙
                     repeatedly(Normalizable[MLQScript].normalizeF(_: MLQScript[Fix[MLQScript]])))
                   .transCata(rewrite.optimize(reflNT))

      _       <- logPhase(PhaseResult.tree("QScript (Optimized)", optmzed.cata(linearize).reverse))
      main    <- plan(optmzed).leftMap(mlerr => mlerr match {
                   case InvalidQName(s) =>
                     FileSystemError.planningFailed(lp, QPlanner.UnsupportedPlan(
                       // TODO: Change to include the QScript context when supported
                       constant(Data.Str(s)), Some(mlerr.shows)))

                   case UnrepresentableEJson(ejs, _) =>
                     FileSystemError.planningFailed(lp, QPlanner.NonRepresentableEJson(ejs.shows))
                 })
      _       <- logPhase(PhaseResult.detail("XQuery", main.render))
    } yield main
  }

  val lpadToLength: FunctionDecl3 =
    declareLocal(NCName("lpad-to-length"))(
      $("padchar") as SequenceType("xs:string"),
      $("length")  as SequenceType("xs:integer"),
      $("str")     as SequenceType("xs:string")
    ).as(SequenceType("xs:string")) { (padchar, length, str) =>
      val (slen, padct, prefix) = ($("slen"), $("padct"), $("prefix"))
      let_(
        slen   := fn.stringLength(str),
        padct  := fn.max(mkSeq_("0".xqy, length - (~slen))),
        prefix := fn.stringJoin(for_($("_") in (1.xqy to ~padct)) return_ padchar, "".xs))
      .return_(
        fn.concat(~prefix, str))
    }

  def saveTo[F[_]: QNameGenerator: PrologW](dst: AFile, results: XQuery): F[XQuery] = {
    val dstDirUri = pathUri(asDir(dst))

    for {
      ts     <- freshName[F]
      i      <- freshName[F]
      result <- freshName[F]
      fname  <- freshName[F]
      now    <- qscript.secondsSinceEpoch[F].apply(fn.currentDateTime)
      dpart  <- lpadToLength[F]("0".xs, 8.xqy, xdmp.integerToHex(~i))
    } yield {
      let_(
        ts     := xdmp.integerToHex(xs.integer(now * 1000.xqy)),
        $("_") := xdmp.directoryCreate(dstDirUri.xs))
      .return_ {
        for_(
          result at i in mkSeq_(results))
        .let_(
          fname := fn.concat(dstDirUri.xs, ~ts, dpart))
        .return_(
          xdmp.documentInsert(~fname, ~result))
      }
    }
  }

  def exec(lp: Fix[LogicalPlan], out: AFile) = {
    import MainModule._

    def saveResults(mm: MainModule): QPlan[MainModule] =
      saveTo[PrologsT[QPlan, ?]](out, mm.queryBody).run map {
        case (plogs, body) =>
          (prologs.modify(_ union plogs) >>> queryBody.set(body))(mm)
      }

    (lpToXQuery(lp) >>= saveResults >>= (mm => liftQP(SessionIO.executeModule_(mm))))
      .as(out).run.run
  }

  def eval(lp: Fix[LogicalPlan]) =
    lpToXQuery(lp).flatMap(main => liftQP(
      ContentSourceIO.resultCursor(
        SessionIO.evaluateModule_(main),
        resultsChunkSize)
    )).run.run

  def explain(lp: Fix[LogicalPlan]) =
    lpToXQuery(lp)
      .map(main => ExecutionPlan(FsType, main.render))
      .run.run

  def exists(file: AFile): Free[S, Boolean] =
    lift(ops.exists(file)).into[S]

  def listContents(dir: ADir): Free[S, FileSystemError \/ Set[PathSegment]] =
    lift(ops.exists(dir).ifM(
      ops.ls(dir).map(_.right[FileSystemError]),
      pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[SessionIO]
    )).into[S]

  queryFileFromDataCursor[S, Task, ResultCursor](exec, eval, explain, listContents, exists)
}


/***
 * Couchbase
 ***/

def interp[S[_]](
    connectionUri: ConnectionUri
  )(implicit
    S0: Task :<: S
  ): DefErrT[Free[S, ?], (Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])] = {

  final case class ConnUriParams(user: String, pass: String)

  def liftDT[A](v: NonEmptyList[String] \/ A): DefErrT[Task, A] =
    EitherT.fromDisjunction[Task](v.leftMap(_.left[EnvironmentError]))

  // TODO: retrieve from connectionUri params
  val env = DefaultCouchbaseEnvironment
    .builder()
    .queryTimeout(SECONDS.toMillis(150))
    .build()

  val cbCtx: DefErrT[Task, Context] =
    for {
      uri     <- liftDT(
                   Uri.fromString(connectionUri.value).leftMap(_.message.wrapNel)
                 )
      cluster <- EitherT(Task.delay(
                   CouchbaseCluster.fromConnectionString(env, uri.renderString).right
                 ).handle {
                   case e: Exception => e.getMessage.wrapNel.left[EnvironmentError].left
                 })
      params  <- liftDT((
                   uri.params.get("username").toSuccessNel("No username in ConnectionUri") |@|
                   uri.params.get("password").toSuccessNel("No password in ConnectionUri")
                 )(ConnUriParams).disjunction)
      cm      =  cluster.clusterManager(params.user, params.pass)
      _       <- EitherT(Task.delay(
                   // verify credentials via call to info
                   cm.info
                 ).as(
                   ().right
                 ).handle {
                   case _: InvalidPasswordException =>
                     invalidCredentials(
                       "Unable to obtain a ClusterManager with provided credentials."
                     ).right[NonEmptyList[String]].left
                   case CBConnectException(ex) =>
                     connectionFailed(
                       ex
                     ).right[NonEmptyList[String]].left
                 })
    } yield Context(cluster, cm)

  def taskInterp(
    ctx: Context
  ): Task[(Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])]  =
    (TaskRef(Map.empty[ReadHandle,   Cursor])          |@|
     TaskRef(Map.empty[WriteHandle,  writefile.State]) |@|
     TaskRef(Map.empty[ResultHandle, Cursor])          |@|
     TaskRef(0L)                                       |@|
     GenUUID.type1
   )((kvR, kvW, kvQ, i, genUUID) =>
    (
      mapSNT(injectNT[Task, S] compose (
        reflNT[Task]                          :+:
        Read.constant[Task, Context](ctx)     :+:
        MonotonicSeq.fromTaskRef(i)           :+:
        genUUID                               :+:
        KeyValueStore.impl.fromTaskRef(kvR)   :+:
        KeyValueStore.impl.fromTaskRef(kvW)   :+:
        KeyValueStore.impl.fromTaskRef(kvQ))),
      lift(Task.delay(ctx.cluster.disconnect()).void).into
    ))

  EitherT(lift(cbCtx.run >>= (_.traverse(taskInterp))).into)
}

package com.gsk.kg.engine.optimizer

import cats.Functor
import cats.implicits._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.{Project => _, _}
import com.gsk.kg.engine.Log
import com.gsk.kg.engine.M
import com.gsk.kg.engine.Phase
import com.gsk.kg.engine.PropertyExpressionF
import com.gsk.kg.engine.PropertyExpressionF._
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.optics._joinR
import com.gsk.kg.engine.optics._pathR
import com.gsk.kg.engine.optics._unionR
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import monocle.POptional

/** This optimization rewrites the DAG for Property Path expressions by performing multiple phases. To explain what
  * every phase tries to achive lets take a look at some example.
  *
  * Here we have some sample query:
  *
  * PREFIX foaf: <http://xmlns.org/foaf/0.1/>
  *
  * SELECT ?s ?o
  * WHERE {
  * ?s (^foaf:knows/^foaf:name/foaf:mbox) ?o .
  * }
  *
  * With the DAG generated when parsed:
  *
  * Project
  * |
  * +- List
  * |  |
  * |  +- VARIABLE(?s)
  * |  |
  * |  `- VARIABLE(?o)
  * |
  * `- Project
  *   |
  *   +- List
  *   |  |
  *   |  +- VARIABLE(?s)
  *   |  |
  *   |  `- VARIABLE(?o)
  *   |
  *   `- Path
  *      |
  *      +- ?s
  *      |
  *      +- SeqExpression
  *      |  |
  *      |  +- SeqExpression
  *      |  |  |
  *      |  |  +- Reverse
  *      |  |  |  |
  *      |  |  |  `- Uri
  *      |  |  |     |
  *      |  |  |     `- <http://xmlns.org/foaf/0.1/knows>
  *      |  |  |
  *      |  |  `- Reverse
  *      |  |     |
  *      |  |     `- Uri
  *      |  |        |
  *      |  |        `- <http://xmlns.org/foaf/0.1/name>
  *      |  |
  *      |  `- Uri
  *      |     |
  *      |     `- <http://xmlns.org/foaf/0.1/mbox>
  *      |
  *      +- ?o
  *      |
  *      +- List(GRAPH_VARIABLE)
  *      |
  *      `- false
  *
  * 1. Reverse pushdown
  * In first place we remove the upper Path expression and all Reverse(pe: PropertyExpression) are pushed down into
  * all the nested Property Expressions until it finds a Uri(s: String) PropertyExpression.
  * This is done by a futumorphism (top-down) that allows to accumulate evaluations done before pushing down the
  * Reverse recursively:
  *
  * SeqExpression
  * |
  * +- SeqExpression
  * |  |
  * |  +- Reverse
  * |  |  |
  * |  |  `- Uri
  * |  |     |
  * |  |     `- <http://xmlns.org/foaf/0.1/knows>
  * |  |
  * |  `- Reverse
  * |     |
  * |     `- Uri
  * |        |
  * |        `- <http://xmlns.org/foaf/0.1/name>
  * |
  * `- Uri
  *   |
  *   `- <http://xmlns.org/foaf/0.1/mbox>
  *
  * 2.PropertyExpression replacements.
  * In the next phase what we do are some replacements on some PropertyExpressions, as follow:
  * - For every Uri expression we replace with a Path expression with reverse field set to false, or if we found
  * a Reverse(Uri) we replace it with a Path with reverse field set to true.
  * - For every SeqExpression we replace for a Join.
  * - For every Alternative we replace with a Union.
  * This phase is done with a anamorphism (top-down) because we need to match Reverse(Uri) before Uri.
  *
  * Join
  * |
  * +- Join
  * |  |
  * |  +- Path
  * |  |  |
  * |  |  +- ?s
  * |  |  |
  * |  |  +- Uri
  * |  |  |  |
  * |  |  |  `- <http://xmlns.org/foaf/0.1/knows>
  * |  |  |
  * |  |  +- ?o
  * |  |  |
  * |  |  +- List(GRAPH_VARIABLE)
  * |  |  |
  * |  |  `- true
  * |  |
  * |  `- Path
  * |     |
  * |     +- ?s
  * |     |
  * |     +- Uri
  * |     |  |
  * |     |  `- <http://xmlns.org/foaf/0.1/name>
  * |     |
  * |     +- ?o
  * |     |
  * |     +- List(GRAPH_VARIABLE)
  * |     |
  * |     `- true
  * |
  * `- Path
  *   |
  *   +- ?s
  *   |
  *   +- Uri
  *   |  |
  *   |  `- <http://xmlns.org/foaf/0.1/mbox>
  *   |
  *   +- ?o
  *   |
  *   +- List(GRAPH_VARIABLE)
  *   |
  *   `- false
  *
  * 3.Chain variables replacement.
  * The last phase is only needed if we have Join expressions from the previous phase. This is because we now that
  * this query, for example this query:
  *
  * PREFIX foaf: <http://xmlns.org/foaf/0.1/>
  *
  * SELECT ?s ?o
  * WHERE {
  * ?s foaf:knows/foaf:name ?o .
  * }
  *
  * Is equivalent to this other one:
  *
  * PREFIX foaf: <http://xmlns.org/foaf/0.1/>
  *
  * SELECT ?s ?o
  * WHERE {
  * ?s foaf:knows ?x1 .
  * ?x1 foaf:name ?o .
  * }
  *
  * Taking a closer look we can see that there is an intermediate variable ?x1 that has to be replaced,
  * so this is the idea of this phase.
  *
  * So, in case we found Join expression, we should create an intermediate variable and replace it for the 'object'
  * variable from the Path of the left of the Join and the 'subject' of the right Path of the Join and we keep doing
  * this between nested joins.
  * This phase is done with an histomorphism (down-top) that will allow to accumulate the result of evaluations done
  * previously in subtrees.
  *
  * Join
  * |
  * +- Join
  * |  |
  * |  +- Path
  * |  |  |
  * |  |  +- ?s
  * |  |  |
  * |  |  +- Uri
  * |  |  |  |
  * |  |  |  `- <http://xmlns.org/foaf/0.1/knows>
  * |  |  |
  * |  |  +- ?_2e2f67aa-628b-4645-9bd3-4b497cabe759
  * |  |  |
  * |  |  +- List(GRAPH_VARIABLE)
  * |  |  |
  * |  |  `- true
  * |  |
  * |  `- Path
  * |     |
  * |     +- ?_2e2f67aa-628b-4645-9bd3-4b497cabe759
  * |     |
  * |     +- Uri
  * |     |  |
  * |     |  `- <http://xmlns.org/foaf/0.1/name>
  * |     |
  * |     +- ?_baceb4e4-3ae5-4c74-9a72-36786c609e84
  * |     |
  * |     +- List(GRAPH_VARIABLE)
  * |     |
  * |     `- true
  * |
  * `- Path
  *   |
  *   +- ?_baceb4e4-3ae5-4c74-9a72-36786c609e84
  *   |
  *   +- Uri
  *   |  |
  *   |  `- <http://xmlns.org/foaf/0.1/mbox>
  *   |
  *   +- ?o
  *   |
  *   +- List(GRAPH_VARIABLE)
  *   |
  *   `- false
  *
  * Finally we can see the hole DAG replaced:
  *
  * Project
  * |
  * +- List
  * |  |
  * |  +- VARIABLE(?s)
  * |  |
  * |  `- VARIABLE(?o)
  * |
  * `- Project
  *   |
  *   +- List
  *   |  |
  *   |  +- VARIABLE(?s)
  *   |  |
  *   |  `- VARIABLE(?o)
  *   |
  *   `- Join
  *      |
  *      +- Join
  *      |  |
  *      |  +- Path
  *      |  |  |
  *      |  |  +- ?s
  *      |  |  |
  *      |  |  +- Uri
  *      |  |  |  |
  *      |  |  |  `- <http://xmlns.org/foaf/0.1/knows>
  *      |  |  |
  *      |  |  +- ?_2e2f67aa-628b-4645-9bd3-4b497cabe759
  *      |  |  |
  *      |  |  +- List(GRAPH_VARIABLE)
  *      |  |  |
  *      |  |  `- true
  *      |  |
  *      |  `- Path
  *      |     |
  *      |     +- ?_2e2f67aa-628b-4645-9bd3-4b497cabe759
  *      |     |
  *      |     +- Uri
  *      |     |  |
  *      |     |  `- <http://xmlns.org/foaf/0.1/name>
  *      |     |
  *      |     +- ?_baceb4e4-3ae5-4c74-9a72-36786c609e84
  *      |     |
  *      |     +- List(GRAPH_VARIABLE)
  *      |     |
  *      |     `- true
  *      |
  *      `- Path
  *         |
  *         +- ?_baceb4e4-3ae5-4c74-9a72-36786c609e84
  *         |
  *         +- Uri
  *         |  |
  *         |  `- <http://xmlns.org/foaf/0.1/mbox>
  *         |
  *         +- ?o
  *         |
  *         +- List(GRAPH_VARIABLE)
  *         |
  *         `- false
  */
object PropertyPathRewrite {

  private def generateRndVariable() =
    VARIABLE(s"?_${java.util.UUID.randomUUID().toString}")

  private def toRecursive[F[_]: Functor, R: Embed[F, *]](
      attr: Attr[F, _]
  ): R = {
    def fix[A](attr: Attr[F, _]): Fix[F] =
      Fix(Functor[F].map(Attr.un(attr)._2)(fix))

    val fixed: Fix[F] = fix(attr)

    scheme[Fix].gcata(Embed[F, R].algebra.gather(Gather.cata)).apply(fixed)
  }

  object RewriteOptics {

    import com.gsk.kg.engine.optics._

    object JoinUpdaters {

      def _joinLeft_PathObject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] =
        _joinR
          .composeLens(Join.l)
          .composePrism(_pathR)
          .composeLens(Path.o)

      def _joinRight_PathObject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] =
        _joinR
          .composeLens(Join.r)
          .composePrism(_pathR)
          .composeLens(Path.o)

      def _joinLeft_PathSubject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] =
        _joinR
          .composeLens(Join.l)
          .composePrism(_pathR)
          .composeLens(Path.s)

      def _joinRight_PathSubject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] =
        _joinR
          .composeLens(Join.r)
          .composePrism(_pathR)
          .composeLens(Path.s)
    }

    object UnionUpdaters {

      def _unionLeftPathSubject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] = _unionR
        .composeLens(Union.l)
        .composePrism(_pathR)
        .composeLens(Path.s)

      def _unionRightPathSubject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] = _unionR
        .composeLens(Union.r)
        .composePrism(_pathR)
        .composeLens(Path.s)

      def _unionLeftPathObject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] = _unionR
        .composeLens(Union.l)
        .composePrism(_pathR)
        .composeLens(Path.o)

      def _unionRightPathObject[T](implicit
          basis: Basis[DAG, T]
      ): POptional[T, T, StringVal, StringVal] = _unionR
        .composeLens(Union.r)
        .composePrism(_pathR)
        .composeLens(Path.o)
    }
  }

  def reverseCoalgebra(implicit
      P: Project[PropertyExpressionF, PropertyExpression]
  ): CVCoalgebra[PropertyExpressionF, PropertyExpression] =
    CVCoalgebra[PropertyExpressionF, PropertyExpression] {
      case Reverse(SeqExpression(pel, per)) =>
        SeqExpressionF(
          Coattr.pure(Reverse(pel)),
          Coattr.pure(Reverse(per))
        )
      case Reverse(Alternative(pel, per)) =>
        AlternativeF(
          Coattr.pure(Reverse(pel)),
          Coattr.pure(Reverse(per))
        )
      case Reverse(OneOrMore(e)) =>
        OneOrMoreF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(ZeroOrMore(e)) =>
        ZeroOrMoreF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(ZeroOrOne(e)) =>
        ZeroOrOneF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(NotOneOf(es)) =>
        NotOneOfF(
          es.map(e =>
            Coattr.pure[PropertyExpressionF, PropertyExpression](Reverse(e))
          )
        )
      case Reverse(BetweenNAndM(n, m, e)) =>
        BetweenNAndMF(n, m, Coattr.pure(Reverse(e)))
      case Reverse(ExactlyN(n, e)) =>
        ExactlyNF(n, Coattr.pure(Reverse(e)))
      case Reverse(NOrMore(n, e)) =>
        NOrMoreF(n, Coattr.pure(Reverse(e)))
      case Reverse(BetweenZeroAndN(n, e)) =>
        BetweenZeroAndNF(n, Coattr.pure(Reverse(e)))
      case x =>
        val a = P.coalgebra
          .apply(x)
        a.map(Coattr.pure)
    }

  def dagAlgebra[T](implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[DAG, T] = {

    import RewriteOptics.JoinUpdaters._
    import RewriteOptics.UnionUpdaters._

    CVAlgebra {
      case Join(
            ll :< Join(_, _),
            _ :< Path(_, per, or, gr, rev)
          ) =>
        val rndVar    = generateRndVariable()
        val updatedLL = _joinRight_PathObject.set(rndVar)(ll)

        joinR(updatedLL, pathR(rndVar, per, or, gr, rev))
      case Join(
            _ :< Path(sl, pel, _, gl, revl),
            _ :< Path(_, per, or, gr, revr)
          ) =>
        val rndVar = generateRndVariable()
        joinR(
          pathR(sl, pel, rndVar, gl, revl),
          pathR(rndVar, per, or, gr, revr)
        )
      case Join(
            _ :< Path(sl, pel, _, gl, rev),
            r :< Union(_, _)
          ) =>
        val rndVar = generateRndVariable()
        val update = (_unionLeftPathSubject.set(rndVar) compose
          _unionRightPathSubject.set(rndVar))(r)

        joinR(
          pathR(sl, pel, rndVar, gl, rev),
          update
        )
      case Join(
            l :< Union(_, _),
            _ :< Path(_, per, or, gr, rev)
          ) =>
        val rndVar = generateRndVariable()
        val update = (_unionLeftPathObject.set(rndVar) compose
          _unionRightPathObject.set(rndVar))(l)

        joinR(
          update,
          pathR(rndVar, per, or, gr, rev)
        )
      case Join(
            l :< Union(_, _),
            r :< Union(_, _)
          ) =>
        val rndVar = generateRndVariable()
        val updateL = (_unionLeftPathObject.set(rndVar) compose
          _unionRightPathObject.set(rndVar))(l)
        val updateR = (_unionLeftPathSubject.set(rndVar) compose
          _unionRightPathSubject.set(rndVar))(r)

        joinR(updateL, updateR)
      case t =>
        t.map(toRecursive(_)).embed
    }
  }

  def pathCoalgebra2[T](
      s: StringVal,
      o: StringVal,
      g: List[StringVal]
  ): CVCoalgebra[DAG, PropertyExpression] =
    CVCoalgebra[DAG, PropertyExpression] {
      case SeqExpression(pel, per) =>
        Join(Coattr.pure(pel), Coattr.pure(per))
      case Alternative(pel, per) =>
        Union(Coattr.pure(pel), Coattr.pure(per))
      case OneOrMore(e) =>
        Path(s, OneOrMore(e), o, g, false)
      case ZeroOrMore(e) =>
        Path(s, ZeroOrMore(e), o, g, false)
      case ZeroOrOne(e) =>
        Path(s, ZeroOrOne(e), o, g, false)
      case NotOneOf(es) =>
        Path(s, NotOneOf(es), o, g, false)
      case BetweenNAndM(n, m, e) =>
        Path(s, BetweenNAndM(n, m, e), o, g, false)
      case ExactlyN(n, e) =>
        Path(s, ExactlyN(n, e), o, g, false)
      case NOrMore(n, e) =>
        Path(s, NOrMore(n, e), o, g, false)
      case BetweenZeroAndN(n, e) =>
        Path(s, BetweenZeroAndN(n, e), o, g, false)
      case Uri(uri) =>
        Path(s, Uri(uri), o, g, false)
      case Reverse(OneOrMore(e)) =>
        Path(s, OneOrMore(Reverse(e)), o, g, false)
      case Reverse(ZeroOrMore(e)) =>
        Path(s, ZeroOrMore(Reverse(e)), o, g, false)
      case Reverse(ZeroOrOne(e)) =>
        Path(s, ZeroOrOne(Reverse(e)), o, g, false)
      case Reverse(NotOneOf(es)) =>
        Path(s, NotOneOf(es.map(Reverse)), o, g, false)
      case Reverse(BetweenNAndM(n, m, e)) =>
        Path(s, BetweenNAndM(n, m, Reverse(e)), o, g, false)
      case Reverse(ExactlyN(n, e)) =>
        Path(s, ExactlyN(n, Reverse(e)), o, g, false)
      case Reverse(NOrMore(n, e)) =>
        Path(s, NOrMore(n, Reverse(e)), o, g, false)
      case Reverse(BetweenZeroAndN(n, e)) =>
        Path(s, BetweenZeroAndN(n, Reverse(e)), o, g, false)
      case Reverse(Uri(uri)) =>
        Path(s, Uri(uri), o, g, true)
    }

  def pathCoalgebra(
      s: StringVal,
      o: StringVal,
      g: List[StringVal]
  ): CoalgebraM[Option, DAG, PropertyExpression] =
    CoalgebraM[Option, DAG, PropertyExpression] {
      case Reverse(Uri(uri)) =>
        Some(Path(s, Uri(uri), o, g, true))
      case Uri(uri) =>
        Some(Path(s, Uri(uri), o, g, false))
      case SeqExpression(pel, per) =>
        Some(Join(pel, per))
      case Alternative(pel, per) =>
        Some(Union(pel, per))
      case _ =>
        None
    }

  def apply[T](implicit
      T: Basis[DAG, T]
  ): T => T = { t =>
    T.coalgebra(t).rewrite { case Path(s, pe, o, g, rev) =>
      val peFutu   = scheme.zoo.futu(reverseCoalgebra)
      val peAna    = scheme.zoo.futu(pathCoalgebra2(s, o, g))
      val dagHisto = scheme.zoo.histo(dagAlgebra)

      val reversedPushedDown = peFutu(pe)
      val unfoldedPaths =
        peAna(reversedPushedDown)
//          .getOrElse(pathR(s, reversedPushedDown, o, g, rev))
      val internalVarReplace = dagHisto(unfoldedPaths)

      T.coalgebra(internalVarReplace)
    }
  }

  def phase[T](implicit T: Basis[DAG, T]): Phase[T, T] = Phase { t =>
    val result = apply(T)(t)
    (result != t)
      .pure[M]
      .ifM(
        Log.debug(
          "Optimizer(PropertyPathRewrite)",
          s"resulting query: ${result.toTree.drawTree}"
        ),
        ().pure[M]
      ) *>
      result.pure[M]
  }
}

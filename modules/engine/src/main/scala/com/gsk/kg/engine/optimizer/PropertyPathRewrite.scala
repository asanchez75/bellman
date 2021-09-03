package com.gsk.kg.engine.optimizer

import cats.Functor
import cats.data.Tuple2K
import cats.implicits._
import com.gsk.kg.engine.DAG.{Project => _, _}
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.Log
import com.gsk.kg.engine.M
import com.gsk.kg.engine.Phase
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import higherkindness.droste.prelude._

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

  def peCoalgebra(s: StringVal, o: StringVal, g: List[StringVal])(implicit
      project: Project[PropertyExpressionF, PropertyExpression]
  ): CVCoalgebra[PropertyExpressionF, PropertyExpression] =
    CVCoalgebra[PropertyExpressionF, PropertyExpression] {
      case Reverse(e) =>
        println(e)
        ReverseF(project.coalgebra.apply(e))
    }

  def dagAlgebra[T](s: StringVal, o: StringVal, g: List[StringVal])(implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[DAG, T] =
    CVAlgebra {
      case Join(
            ll :< Join(_, _),
            _ :< Path(_, per, or, gr, rev)
          ) =>
        import com.gsk.kg.engine.optics._

        val updater = _joinR
          .composeLens(Join.r)
          .composePrism(_pathR)
          .composeLens(Path.o)

        val rndVar    = generateRndVariable()
        val updatedLL = updater.set(rndVar)(ll)

        joinR(updatedLL, pathR(rndVar, per, or, gr))
      case Join(
            _ :< Path(sl, pel, _, gl, revl),
            _ :< Path(_, per, or, gr, revr)
          ) =>
        val rndVar = generateRndVariable()
        joinR(
          pathR(sl, pel, rndVar, gl, revl),
          pathR(rndVar, per, or, gr, revr)
        )
      case Wrap(Reverse(e)) =>
        pathR(s, e, o, g, true)
      case t =>
        t.map(toRecursive(_)).embed
    }

  def altAlgebra[T](s: StringVal, o: StringVal, g: List[StringVal])(implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[PropertyExpressionF, T] =
    CVAlgebra[PropertyExpressionF, T] {
      case AlternativeF(
            l :< AlternativeF(_, _),
            r :< ReverseF(pe)
          ) =>
        unionR(l, pathR(s, toRecursive(pe), o, g, true))
      case AlternativeF(
            Wrap(_) :< ReverseF(fpel),
            Wrap(_) :< ReverseF(fper)
          ) =>
        unionR(
          pathR(s, toRecursive(fpel), o, g, true),
          pathR(s, toRecursive(fper), o, g, true)
        )
      case AlternativeF(
            Wrap(_) :< ReverseF(fpel),
            Wrap(per) :< UriF(_)
          ) =>
        unionR(
          pathR(s, toRecursive(fpel), o, g, true),
          pathR(s, per, o, g)
        )
      case AlternativeF(
            Wrap(pel) :< UriF(_),
            Wrap(_) :< ReverseF(fper)
          ) =>
        unionR(
          pathR(s, pel, o, g),
          pathR(s, toRecursive(fper), o, g, true)
        )
      case AlternativeF(
            Wrap(pel) :< UriF(_),
            Wrap(per) :< UriF(_)
          ) =>
        unionR(
          pathR(s, pel, o, g),
          pathR(s, per, o, g)
        )
      case AlternativeF(dagL :< _, per) =>
        val a = toRecursive(per)
        unionR(
          dagL,
          pathR(s, a, o, g)
        )
//      case ReverseF(
//            Union(
//              Path(sl, pel, ol, gl, _),
//              Path(sr, per, or, gr, _)
//            ) :< _
//          ) =>
//        import com.gsk.kg.engine.optics._
//
//        val updater = _pathR.composeLens(Path.reverse)
//        val l       = updater.set(true)(pel.asInstanceOf[T])
//        val r       = updater.set(true)(per.asInstanceOf[T])

//        unionR(pathR(sl, pel, ol, gl, true), pathR(sr, per, or, gr, true))
      case ReverseF(Wrap(pe) :< _) =>
        wrapR(Reverse(pe))
      case ReverseF(Wrap(pe) :< UriF(_)) =>
        pathR(s, pe, o, g, true)
      case UriF(x) =>
        wrapR(Uri(x))
      case pe =>
        pathR(s, pe.map(toRecursive(_)).embed, o, g)
    }

  def seqAlgebra[T](s: StringVal, o: StringVal, g: List[StringVal])(implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[PropertyExpressionF, T] =
    CVAlgebra[PropertyExpressionF, T] {
      case SeqExpressionF(
            Wrap(_) :< ReverseF(fpel),
            Wrap(_) :< ReverseF(fper)
          ) =>
        joinR(
          pathR(s, toRecursive(fpel), o, g, true),
          pathR(s, toRecursive(fper), o, g, true)
        )
      case SeqExpressionF(
            Wrap(_) :< ReverseF(fpel),
            Wrap(per) :< UriF(_)
          ) =>
        joinR(
          pathR(s, toRecursive(fpel), o, g, true),
          pathR(s, per, o, g)
        )
      case SeqExpressionF(
            Wrap(pel) :< UriF(_),
            Wrap(_) :< ReverseF(fper)
          ) =>
        joinR(
          pathR(s, pel, o, g),
          pathR(s, toRecursive(fper), o, g, true)
        )
      case SeqExpressionF(
            Wrap(pel) :< UriF(_),
            Wrap(per) :< UriF(_)
          ) =>
        joinR(
          pathR(s, pel, o, g),
          pathR(s, per, o, g)
        )
      case SeqExpressionF(dagL :< _, per) =>
        joinR(
          dagL,
          pathR(s, toRecursive(per), o, g)
        )
//      case ReverseF(
//            Join(
//              pel,
//              per
//            ) :< _
//          ) =>
//        import com.gsk.kg.engine.optics._
//
//        val updater = _pathR.composeLens(Path.reverse)
//        val l       = updater.set(true)(pel.asInstanceOf[T])
//        val r       = updater.set(true)(per.asInstanceOf[T])
//
//        joinR(l, r)
      case ReverseF(Wrap(pe) :< _) =>
        wrapR(Reverse(pe))
      case ReverseF(Wrap(pe) :< UriF(_)) =>
        pathR(s, pe, o, g, true)
      case UriF(x) =>
        wrapR(Uri(x))
      case pe =>
        pathR(s, pe.map(toRecursive(_)).embed, o, g)
    }

  def apply[T, P](implicit
      T: Basis[DAG, T],
      P: Basis[PropertyExpressionF, P]
  ): T => T = { t =>
    T.coalgebra(t).rewrite { case Path(s, pe, o, g, _) =>
//      val peFutu = scheme.zoo.futu(peCoalgebra[PropertyExpression](s, o, g))
//      val a = peFutu()

//      val peHisto = scheme.zoo.histo(altAlgebra(s, o, g))
      val peHisto  = scheme.zoo.histo(seqAlgebra(s, o, g))
      val dagHisto = scheme.zoo.histo(dagAlgebra(s, o, g))
      T.coalgebra(dagHisto(peHisto(pe)))
    }
  }

  def phase[T](implicit T: Basis[DAG, T]): Phase[T, T] = Phase { t =>
    val result = apply(T)(t)
    (result != t)
      .pure[M]
      .ifM(
        Log.debug(
          "Optimizer(ReversePathRewrite)",
          s"resulting query: ${result.toTree.drawTree}"
        ),
        ().pure[M]
      ) *>
      result.pure[M]
  }
}

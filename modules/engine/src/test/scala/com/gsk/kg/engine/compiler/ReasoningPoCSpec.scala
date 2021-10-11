package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.sparqlparser.TestConfig
import higherkindness.droste.util.newtypes.@@
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReasoningPoCSpec
    extends AnyWordSpec
    with Matchers
    with TestConfig
    with SparkSpec {

  import sqlContext.implicits._

  // Ontology
  lazy val schema = List(
    (
      "<http://example.org/class/Person>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://www.w3.org/2002/07/owl#Class>"
    ),
    (
      "<http://example.org/class/Author>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://www.w3.org/2002/07/owl#Class>"
    ),
    (
      "<http://example.org/class/Author>",
      "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
      "<http://example.org/class/Person>"
    ),
    (
      "<http://example.org/class/Biologist>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://www.w3.org/2002/07/owl#Class>"
    ),
    (
      "<http://example.org/class/Biologist>",
      "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
      "<http://example.org/class/Person>"
    ),
    (
      "<http://example.org/class/Engineer>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://www.w3.org/2002/07/owl#Class>"
    ),
    (
      "<http://example.org/class/Engineer>",
      "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
      "<http://example.org/class/Person>"
    ),
    (
      "<http://example.org/class/Senior_Engineer>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://www.w3.org/2002/07/owl#Class>"
    ),
    (
      "<http://example.org/class/Senior_Engineer>",
      "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
      "<http://example.org/class/Engineer>"
    )
  ).toDF("s", "p", "o")

  lazy val data = List(
    (
      "<http://example.org/data/Herman>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Author>"
    ),
    (
      "<http://example.org/data/Toni>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Author>"
    ),
    (
      "<http://example.org/data/Alice>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Biologist>"
    ),
    (
      "<http://example.org/data/Bob>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Engineer>"
    ),
    (
      "<http://example.org/data/Marcel>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Engineer>"
    ),
    (
      "<http://example.org/data/Tom>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://example.org/class/Senior_Engineer>"
    )
  ).toDF("s", "p", "o")

  val initialQuery =
    """
      |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      |PREFIX : <http://example.org/class/>
      |
      |SELECT ?person
      |WHERE {
      | ?person rdf:type :Person
      |}
      |""".stripMargin

  "subClassOf" should {

    "forward chaining (SANSA)" in {

      /*
        Query for rdfs entitlement 11:
        xxx rdfs:subClassOf yyy . | xxx rdfs:subClassOf zzz .
        yyy rdfs:subClassOf zzz . |
       */
      val rdfs11Query =
        """
          |PREFIX owl: <http://www.w3.org/2002/07/owl#>
          |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          |
          |CONSTRUCT { ?x rdfs:subClassOf ?z }
          |WHERE {
          | ?x rdfs:subClassOf ?y .
          | ?y rdfs:subClassOf ?z .
          |}
          |""".stripMargin

      val rdfs11QueryResult =
        Compiler.compile(schema union data, rdfs11Query, config)
      val rdfs11 = rdfs11QueryResult.right.get

      println(
        "rdfs11: xxx rdfs:subClassOf yyy . yyy rdfs:subClassOf zzz . => xxx subClassOf zzz ."
      )
      rdfs11.show(false)

      /*
       Query for rdfs entitlement 9:
        xxx rdfs:subClassOf yyy . | zzz rdf:type yyy
        zzz rdf:class xxx .       |
       */
      val rdfs9Query =
        """
          |PREFIX owl: <http://www.w3.org/2002/07/owl#>
          |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          |
          |CONSTRUCT { ?z rdf:type ?y }
          |WHERE {
          | ?x rdfs:subClassOf ?y .
          | ?z rdf:type ?x .
          |}
          |""".stripMargin

      val rdfs9QueryResult =
        Compiler.compile(schema union data union rdfs11, rdfs9Query, config)
      val rdfs9 = rdfs9QueryResult.right.get

      println(
        "rdfs9: xxx rdfs:subClassOf yyy . zzz rdf:type xxx . => zzz rdf:type yyy ."
      )
      rdfs9.show(false)

      val schemaPlusEntitlementsPlusData =
        schema union rdfs9 union rdfs11 union data
      val queryResult =
        Compiler.compile(schemaPlusEntitlementsPlusData, initialQuery, config)
      val result = queryResult.right.get
      result.show(false)

      result.collect().toSeq should contain theSameElementsAs Seq(
        Row("<http://example.org/data/Herman>"),
        Row("<http://example.org/data/Toni>"),
        Row("<http://example.org/data/Alice>"),
        Row("<http://example.org/data/Bob>"),
        Row("<http://example.org/data/Marcel>"),
        Row("<http://example.org/data/Tom>")
      )
    }

    "backward chaining (Stardog)" in {

      // Step 1: Analyze query triples:
      // IF any triple 'p' == rdf:type THEN get 'o' as targetClass.
      val targetClass = ":Person"

      // rdfs11

      def rdfs11Query =
        """
          |PREFIX owl: <http://www.w3.org/2002/07/owl#>
          |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
          |PREFIX : <http://example.org/class/>
          |
          |SELECT ?x ?z
          |WHERE {
          | ?x rdfs:subClassOf ?y .
          | ?y rdfs:subClassOf ?z .
          |}
          |""".stripMargin

      val rdfs11QueryResult = Compiler
        .compile(schema union data, rdfs11Query, config)
        .right
        .get

      println("rdfs11:")
      rdfs11QueryResult.show(false)

      // Step 2: Get any subClass for targetClass (this should be done recursively to find hierarchies DFS)
      // Another way to see it is:
      // rdfs9: SubclassOf(x, y) ^ Type(z, x) => Type(z, y)
      // query: Type(z, Person)
      // So we go backwards on the horn clause by substituting the rhs of the horn clause with the query:
      // SubclassOf(x, Person) ^ Type(z, x)  => Type(z, Person)
      // And we should search backwards on the lhs by searching the solutions that satisfy the literals.
      def rdfs9Query(targetClass: String): String =
        s"""
           |PREFIX owl: <http://www.w3.org/2002/07/owl#>
           |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
           |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           |PREFIX : <http://example.org/class/>
           |
           |SELECT ?x
           |WHERE {
           | ?x rdfs:subClassOf $targetClass .
           | ?z rdf:type ?x .
           |}
           |""".stripMargin

      val rdfs9QueryResult =
        Compiler
          .compile(schema union data, rdfs9Query(targetClass), config)
          .right
          .get

      println("rdfs9:")
      rdfs9QueryResult.show(false)

      // Step 3: For each subClassOf generate a new query
      val rdfs9 = rdfs9QueryResult
        .collect()
        .toSeq
        .flatMap(_.toSeq.map(_.asInstanceOf[String]))

      val initialQueryTemplate = rdfs9.map { subClass =>
        s"""
          |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
          |PREFIX : <http://example.org/class/>
          |
          |SELECT ?person
          |WHERE {
          | ?person rdf:type $subClass
          |}
          |""".stripMargin
      }

      println("Queries:")
      initialQueryTemplate.foreach(println)

      // Step 4: Execute initial query + subclasses queries over data:
      val queriesResults =
        (initialQuery +: initialQueryTemplate).zipWithIndex.map {
          case (q, idx) =>
            val result = Compiler.compile(data, q, config).right.get
            println(s"Query result $idx:")
            result.show(false)
            result
        }

      // Step 5: We union all results and discard duplicates
      import com.gsk.kg.engine.relational.Relational.Untyped
      import com.gsk.kg.engine.relational.Relational.ops._

      val structSchema = StructType(Seq(StructField("?person", StringType)))

      val result = queriesResults
        .foldLeft(
          Relational[DataFrame @@ Untyped].emptyWithSchema(structSchema)
        ) { case (acc, elem) =>
          acc.union(@@(elem))
        }
        .distinct
        .unwrap

      result.show(false)

      succeed
    }
  }
}

# SPARQL Query Testing Recommendations for Bellman

## Executive Summary

Based on analysis of Bellman's existing test infrastructure, this document provides comprehensive recommendations for implementing tests to evaluate SPARQL query support. The recommendations cover multiple testing strategies, from unit tests to integration tests and W3C compliance tests.

---

## Table of Contents

1. [Current Test Infrastructure Overview](#current-test-infrastructure-overview)
2. [Testing Strategy Recommendations](#testing-strategy-recommendations)
3. [Test Patterns and Best Practices](#test-patterns-and-best-practices)
4. [Implementation Guide](#implementation-guide)
5. [Test Organization](#test-organization)
6. [Example Implementations](#example-implementations)
7. [Performance and Regression Testing](#performance-and-regression-testing)
8. [Continuous Integration](#continuous-integration)

---

## Current Test Infrastructure Overview

### Existing Test Structure

Bellman currently has three types of tests:

**1. Unit Tests** (`modules/engine/src/test/scala/`)
- **98 test spec files** covering individual SPARQL features
- Pattern: ScalaTest with `AnyWordSpec` and `Matchers`
- Focus: Feature-specific testing (GROUP BY, HAVING, FILTER, etc.)
- Location: `/home/user/bellman/modules/engine/src/test/scala/com/gsk/kg/engine/compiler/`

**2. Parser Tests** (`modules/parser/src/test/scala/`)
- Tests for SPARQL parsing into ADTs
- Validates syntax parsing without execution
- Uses Apache Jena for reference parsing

**3. W3C Compliance Tests** (`modules/rdf-tests/`)
- Automated tests from W3C SPARQL 1.1 test suite
- **Current pass rate: 3% (7/179 tests)**
- Location: `/home/user/bellman/modules/rdf-tests/src/test/scala/com/gsk/kg/RdfTests.scala`

### Test Infrastructure Components

**Key Files:**
```
modules/engine/src/test/scala/com/gsk/kg/engine/compiler/
├── SparkSpec.scala           # Base trait for Spark testing
├── GroupBySpec.scala         # Example feature test
├── HavingSpec.scala         # Example feature test
└── [96 other spec files]

modules/parser/src/test/scala/com/gsk/kg/sparqlparser/
├── TestConfig.scala         # Test configuration
└── [various parser tests]

modules/rdf-tests/src/test/scala/com/gsk/kg/
└── RdfTests.scala          # W3C compliance suite
```

---

## Testing Strategy Recommendations

### 1. Multi-Level Testing Pyramid

```
                    ┌──────────────────┐
                    │  Compliance      │  W3C SPARQL 1.1 Tests
                    │  Tests (3%)      │  Goal: Increase coverage
                    └──────────────────┘
                   ┌────────────────────┐
                   │  Integration Tests │  End-to-end query scenarios
                   │  (Recommended)     │  Real-world use cases
                   └────────────────────┘
              ┌──────────────────────────────┐
              │    Feature Tests (98 specs)  │  Existing coverage
              │    GROUP BY, FILTER, etc.    │  Continue expanding
              └──────────────────────────────┘
         ┌────────────────────────────────────────┐
         │         Unit Tests                     │  Individual components
         │         Parser, Optimizer, etc.        │  Fast, isolated
         └────────────────────────────────────────┘
```

### 2. Recommended Testing Approaches

#### A. Feature-Based Testing (Current Strength)
**Continue and expand** - This is working well

**Pattern:**
```scala
class FeatureSpec extends AnyWordSpec with Matchers with SparkSpec {
  "Feature Name" should {
    "handle basic case" in { /* test */ }
    "handle edge case 1" in { /* test */ }
    "handle edge case 2" in { /* test */ }
    "fail appropriately for invalid input" in { /* test */ }
  }
}
```

**Recommended features to test:**
- ✅ Already tested: GROUP BY, HAVING, FILTER, OPTIONAL, UNION, etc.
- 🔲 Need more coverage: Property paths, Subqueries, BIND, VALUES
- 🔲 Need improvement: String functions, Date/Time functions, Hash functions

#### B. Query Complexity Testing (Recommended Addition)
**Add tests** that combine multiple features

**Rationale:** Real queries use multiple features together

**Pattern:**
```scala
class ComplexQuerySpec extends AnyWordSpec with Matchers with SparkSpec {
  "complex queries" should {
    "combine GROUP BY with FILTER and ORDER BY" in { /* test */ }
    "use OPTIONAL with aggregates" in { /* test */ }
    "nest multiple subqueries" in { /* test */ }
  }
}
```

#### C. Data-Driven Testing (Recommended Addition)
**Create reusable test datasets** with known characteristics

**Pattern:**
```scala
object TestDatasets {
  def socialNetworkData: DataFrame = /* friends, knows, etc. */
  def bibliographicData: DataFrame = /* papers, authors, citations */
  def temporalData: DataFrame = /* events with timestamps */
}

class SocialNetworkQueriesSpec extends AnyWordSpec with SparkSpec {
  lazy val df = TestDatasets.socialNetworkData

  "social network queries" should {
    "find friends of friends" in { /* test */ }
    "calculate network centrality" in { /* test */ }
  }
}
```

#### D. Negative Testing (Needs More Coverage)
**Test error handling and invalid queries**

**Pattern:**
```scala
"error handling" should {
  "reject malformed SPARQL" in {
    val query = "SELECT ?x WHERE { ?x ?p }" // missing triple end
    val result = Compiler.compile(df, query, config)
    result shouldBe a[Left[_, _]]
  }

  "handle type mismatches gracefully" in {
    val query = """SELECT ?x WHERE { ?x <p> ?y FILTER(?y > "string") }"""
    val result = Compiler.compile(df, query, config)
    // Should either fail gracefully or handle type coercion
  }
}
```

#### E. Differential Testing (Recommended Addition)
**Compare Bellman results with reference implementations**

**Pattern:**
```scala
class DifferentialTestingSpec extends AnyWordSpec with SparkSpec {
  "bellman results" should {
    "match Apache Jena results" in {
      val query = /* SPARQL query */
      val bellmanResult = runInBellman(df, query)
      val jenaResult = runInJena(rdfModel, query)

      bellmanResult shouldEqual jenaResult
    }
  }
}
```

**Tools:**
- Apache Jena (already used for parsing)
- RDF4J (another SPARQL engine)
- Blazegraph (graph database with SPARQL)

---

## Test Patterns and Best Practices

### Pattern 1: Basic Feature Test

```scala
package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MyFeatureSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "my feature" should {

    "handle basic case" in {
      // 1. Setup test data
      val df = List(
        ("<http://example.org/s1>", "<http://example.org/p1>", "\"value1\""),
        ("<http://example.org/s2>", "<http://example.org/p1>", "\"value2\"")
      ).toDF("s", "p", "o")

      // 2. Define SPARQL query
      val query = """
        PREFIX ex: <http://example.org/>
        SELECT ?s ?o
        WHERE {
          ?s ex:p1 ?o
        }
      """

      // 3. Execute
      val result = Compiler.compile(df, query, config)

      // 4. Assert
      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual Set(
        Row("<http://example.org/s1>", "\"value1\""),
        Row("<http://example.org/s2>", "\"value2\"")
      )
    }

    "handle edge case with empty results" in {
      val df = List.empty[(String, String, String)].toDF("s", "p", "o")
      val query = "SELECT ?s WHERE { ?s ?p ?o }"
      val result = Compiler.compile(df, query, config)

      result.right.get.collect shouldBe empty
    }
  }
}
```

### Pattern 2: Parameterized Testing

```scala
class ParameterizedSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  val testCases = Table(
    ("description", "query", "expected"),
    (
      "simple filter",
      "SELECT ?x WHERE { ?x <p> ?y FILTER(?y > 5) }",
      Set(Row("<http://ex.org/a>"))
    ),
    (
      "complex filter",
      "SELECT ?x WHERE { ?x <p> ?y FILTER(?y > 5 && ?y < 10) }",
      Set(Row("<http://ex.org/b>"))
    )
  )

  val df = List(
    ("<http://ex.org/a>", "<p>", "\"10\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    ("<http://ex.org/b>", "<p>", "\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>")
  ).toDF("s", "p", "o")

  forAll(testCases) { (description, query, expected) =>
    s"execute $description correctly" in {
      val result = Compiler.compile(df, query, config)
      result.right.get.collect.toSet shouldEqual expected
    }
  }
}
```

### Pattern 3: Shared Test Fixtures

```scala
trait CommonTestData extends SparkSpec {
  import sqlContext.implicits._

  lazy val peopleDF = List(
    ("<http://ex.org/alice>", "<http://ex.org/name>", "\"Alice\""),
    ("<http://ex.org/alice>", "<http://ex.org/age>", "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    ("<http://ex.org/bob>", "<http://ex.org/name>", "\"Bob\""),
    ("<http://ex.org/bob>", "<http://ex.org/age>", "\"25\"^^<http://www.w3.org/2001/XMLSchema#integer>")
  ).toDF("s", "p", "o")

  lazy val relationshipsDF = List(
    ("<http://ex.org/alice>", "<http://ex.org/knows>", "<http://ex.org/bob>"),
    ("<http://ex.org/bob>", "<http://ex.org/knows>", "<http://ex.org/carol>")
  ).toDF("s", "p", "o")
}

class SocialQuerySpec extends AnyWordSpec with Matchers with CommonTestData with TestConfig {
  "social queries" should {
    "find people and their ages" in {
      val query = "SELECT ?name ?age WHERE { ?p <http://ex.org/name> ?name . ?p <http://ex.org/age> ?age }"
      val result = Compiler.compile(peopleDF, query, config)
      // assertions
    }
  }
}
```

### Pattern 4: Testing with Different RDF Data Types

```scala
class DatatypeSpec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  "datatype handling" should {

    val df = List(
      ("<http://ex.org/x>", "<http://ex.org/intProp>", "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
      ("<http://ex.org/x>", "<http://ex.org/floatProp>", "\"3.14\"^^<http://www.w3.org/2001/XMLSchema#float>"),
      ("<http://ex.org/x>", "<http://ex.org/stringProp>", "\"hello\""),
      ("<http://ex.org/x>", "<http://ex.org/dateProp>", "\"2023-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>"),
      ("<http://ex.org/x>", "<http://ex.org/boolProp>", "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
    ).toDF("s", "p", "o")

    "handle integer operations" in {
      val query = """
        SELECT ?x ?result
        WHERE {
          ?x <http://ex.org/intProp> ?val .
          BIND(?val + 10 AS ?result)
        }
      """
      // test
    }

    "handle string operations" in {
      val query = """
        SELECT ?x ?upper
        WHERE {
          ?x <http://ex.org/stringProp> ?val .
          BIND(UCASE(?val) AS ?upper)
        }
      """
      // test
    }
  }
}
```

---

## Implementation Guide

### Step 1: Choose Your Testing Approach

**For new SPARQL features:**
```
1. Create a new *Spec.scala file in modules/engine/src/test/scala/com/gsk/kg/engine/compiler/
2. Follow the Pattern 1 structure
3. Test basic functionality, edge cases, and error conditions
```

**For integration testing:**
```
1. Create test datasets in a TestDatasets object
2. Write queries that combine multiple features
3. Verify results against known expected outputs
```

**For W3C compliance:**
```
1. The infrastructure already exists in modules/rdf-tests
2. Focus on fixing failing tests
3. Use the test results in rdf-test-results.md to prioritize
```

### Step 2: Set Up Test Environment

**Base Test Class:**

```scala
// File: modules/engine/src/test/scala/com/gsk/kg/engine/compiler/MyFeatureSpec.scala

package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MyFeatureSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec      // Provides Spark context
    with TestConfig {   // Provides default config

  import sqlContext.implicits._

  // Your tests here
}
```

**SparkSpec** provides:
- Local Spark session
- SQLContext
- Proper cleanup after tests

**TestConfig** provides:
- Default Bellman configuration
- Can be overridden per test if needed

### Step 3: Write Test Data

**Option A: Inline Data (for small tests)**

```scala
val df = List(
  ("<http://ex.org/s1>", "<http://ex.org/p1>", "\"object1\""),
  ("<http://ex.org/s2>", "<http://ex.org/p1>", "\"object2\"")
).toDF("s", "p", "o")
```

**Option B: Load from Files (for complex tests)**

```scala
def loadTestData(filename: String): DataFrame = {
  import org.apache.jena.riot.RDFParser
  import org.apache.jena.riot.lang.CollectorStreamTriples
  import scala.collection.JavaConverters._

  val inputStream = new CollectorStreamTriples()
  RDFParser.source(s"modules/engine/src/test/resources/$filename").parse(inputStream)

  inputStream.getCollected().asScala.toList
    .map(triple => (
      triple.getSubject().toString(),
      triple.getPredicate().toString(),
      triple.getObject().toString()
    ))
    .toDF("s", "p", "o")
}
```

**Option C: Programmatic Test Data Generation**

```scala
object TestDataGenerators {
  def generateSocialNetwork(numPeople: Int): DataFrame = {
    val people = (1 to numPeople).flatMap { i =>
      List(
        (s"<http://ex.org/person$i>", "<http://ex.org/name>", s"\"Person$i\""),
        (s"<http://ex.org/person$i>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://ex.org/Person>")
      )
    }
    people.toDF("s", "p", "o")
  }
}
```

### Step 4: Write and Execute Queries

```scala
"feature" should {
  "work correctly" in {
    // 1. Setup
    val df = /* your test data */

    // 2. Query
    val query = """
      PREFIX ex: <http://example.org/>
      SELECT ?s ?o
      WHERE {
        ?s ex:property ?o
      }
    """

    // 3. Execute
    val result = Compiler.compile(df, query, config)

    // 4. Verify success
    result shouldBe a[Right[_, _]]

    // 5. Check results
    val actualResults = result.right.get.collect.toSet
    val expectedResults = Set(
      Row("<http://ex.org/s1>", "\"value1\""),
      Row("<http://ex.org/s2>", "\"value2\"")
    )

    actualResults shouldEqual expectedResults
  }
}
```

### Step 5: Assert Results

**Common Assertion Patterns:**

```scala
// Success/Failure
result shouldBe a[Right[_, _]]  // Query succeeded
result shouldBe a[Left[_, _]]   // Query failed

// Exact match
result.right.get.collect.toSet shouldEqual expectedSet

// Partial match
result.right.get.collect should contain allOf (Row(...), Row(...))

// Size check
result.right.get.collect should have size 5

// Empty results
result.right.get.collect shouldBe empty

// Column names
result.right.get.columns should contain allOf ("?x", "?y")

// Row structure
result.right.get.schema.fields should have length 3
```

---

## Test Organization

### Recommended File Structure

```
modules/
├── engine/src/test/scala/com/gsk/kg/engine/
│   ├── compiler/
│   │   ├── feature/              # Feature-specific tests (existing)
│   │   │   ├── GroupBySpec.scala
│   │   │   ├── HavingSpec.scala
│   │   │   └── ...
│   │   ├── integration/          # Integration tests (NEW)
│   │   │   ├── ComplexQuerySpec.scala
│   │   │   ├── RealWorldUseCasesSpec.scala
│   │   │   └── ...
│   │   ├── regression/           # Regression tests (NEW)
│   │   │   ├── Issue123Spec.scala
│   │   │   ├── Issue456Spec.scala
│   │   │   └── ...
│   │   └── performance/          # Performance tests (NEW)
│   │       ├── LargeDatasetSpec.scala
│   │       └── QueryPerformanceSpec.scala
│   ├── fixtures/                 # Test data and utilities
│   │   ├── TestDatasets.scala
│   │   ├── QueryExamples.scala
│   │   └── TestHelpers.scala
│   └── resources/
│       ├── test-data/            # RDF files for testing
│       │   ├── simple.nt
│       │   ├── complex.nt
│       │   └── ...
│       └── queries/              # SPARQL query files
│           ├── aggregate-queries.sparql
│           └── ...
└── rdf-tests/                    # W3C compliance (existing)
    └── src/test/scala/com/gsk/kg/RdfTests.scala
```

### Naming Conventions

**Test Classes:**
- Feature tests: `[Feature]Spec.scala` (e.g., `GroupBySpec.scala`)
- Integration tests: `[UseCase]Spec.scala` (e.g., `ComplexQuerySpec.scala`)
- Regression tests: `Issue[Number]Spec.scala` or `Bug[Description]Spec.scala`

**Test Methods:**
- Use descriptive names: `"handle GROUP BY with multiple variables"`
- Use behavioral style: `"return empty results when no data matches"`
- Use edge case descriptions: `"handle NULL values in aggregates correctly"`

---

## Example Implementations

### Example 1: Complete Feature Test

```scala
package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DistinctSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "DISTINCT modifier" should {

    "remove duplicate results" in {
      val df = List(
        ("<http://ex.org/alice>", "<http://ex.org/name>", "\"Alice\""),
        ("<http://ex.org/alice>", "<http://ex.org/name>", "\"Alice\""),
        ("<http://ex.org/bob>", "<http://ex.org/name>", "\"Bob\""),
        ("<http://ex.org/bob>", "<http://ex.org/name>", "\"Bob\"")
      ).toDF("s", "p", "o")

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT DISTINCT ?name
        WHERE {
          ?person ex:name ?name
        }
      """

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Alice\""),
        Row("\"Bob\"")
      )
    }

    "work with multiple columns" in {
      val df = List(
        ("<http://ex.org/alice>", "<http://ex.org/name>", "\"Alice\""),
        ("<http://ex.org/alice>", "<http://ex.org/age>", "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        ("<http://ex.org/bob>", "<http://ex.org/name>", "\"Alice\""),
        ("<http://ex.org/bob>", "<http://ex.org/age>", "\"25\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      ).toDF("s", "p", "o")

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT DISTINCT ?name ?age
        WHERE {
          ?person ex:name ?name .
          ?person ex:age ?age
        }
      """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect should have size 2
    }

    "handle empty results" in {
      val df = List.empty[(String, String, String)].toDF("s", "p", "o")

      val query = """
        SELECT DISTINCT ?x
        WHERE { ?x ?p ?o }
      """

      val result = Compiler.compile(df, query, config)

      result.right.get.collect shouldBe empty
    }
  }
}
```

### Example 2: Integration Test

```scala
package com.gsk.kg.engine.compiler.integration

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BibliographicQueriesSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  lazy val bibliographicData = List(
    // Papers
    ("<http://ex.org/paper1>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://ex.org/Paper>"),
    ("<http://ex.org/paper1>", "<http://ex.org/title>", "\"Scala Programming\""),
    ("<http://ex.org/paper1>", "<http://ex.org/year>", "\"2010\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    ("<http://ex.org/paper2>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<http://ex.org/Paper>"),
    ("<http://ex.org/paper2>", "<http://ex.org/title>", "\"Functional Programming\""),
    ("<http://ex.org/paper2>", "<http://ex.org/year>", "\"2015\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    // Authors
    ("<http://ex.org/paper1>", "<http://ex.org/author>", "<http://ex.org/alice>"),
    ("<http://ex.org/paper2>", "<http://ex.org/author>", "<http://ex.org/alice>"),
    ("<http://ex.org/paper2>", "<http://ex.org/author>", "<http://ex.org/bob>"),
    ("<http://ex.org/alice>", "<http://ex.org/name>", "\"Alice Smith\""),
    ("<http://ex.org/bob>", "<http://ex.org/name>", "\"Bob Jones\""),
    // Citations
    ("<http://ex.org/paper2>", "<http://ex.org/cites>", "<http://ex.org/paper1>")
  ).toDF("s", "p", "o")

  "bibliographic queries" should {

    "find authors with their publication counts" in {
      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?authorName (COUNT(?paper) AS ?paperCount)
        WHERE {
          ?paper a ex:Paper .
          ?paper ex:author ?author .
          ?author ex:name ?authorName .
        }
        GROUP BY ?authorName
        ORDER BY DESC(?paperCount)
      """

      val result = Compiler.compile(bibliographicData, query, config)

      result shouldBe a[Right[_, _]]
      val rows = result.right.get.collect

      rows should have size 2
      rows(0)(0) shouldEqual "\"Alice Smith\""  // Most papers
    }

    "find papers published after 2012 with author names" in {
      val query = """
        PREFIX ex: <http://ex.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?title ?authorName
        WHERE {
          ?paper a ex:Paper .
          ?paper ex:title ?title .
          ?paper ex:year ?year .
          ?paper ex:author ?author .
          ?author ex:name ?authorName .
          FILTER(?year > "2012"^^xsd:integer)
        }
      """

      val result = Compiler.compile(bibliographicData, query, config)

      result.right.get.collect should have size 2  // Paper2 has 2 authors
    }

    "find citation relationships" in {
      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?citingTitle ?citedTitle
        WHERE {
          ?citingPaper ex:cites ?citedPaper .
          ?citingPaper ex:title ?citingTitle .
          ?citedPaper ex:title ?citedTitle .
        }
      """

      val result = Compiler.compile(bibliographicData, query, config)

      result.right.get.collect.toSet shouldEqual Set(
        Row("\"Functional Programming\"", "\"Scala Programming\"")
      )
    }
  }
}
```

### Example 3: Regression Test

```scala
package com.gsk.kg.engine.compiler.regression

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Issue202GroupConcatSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "Issue #202: GROUP_CONCAT implementation" should {

    "concatenate values with default separator" ignore {  // Use 'ignore' for not-yet-implemented
      val df = List(
        ("<http://ex.org/person1>", "<http://ex.org/hobby>", "\"reading\""),
        ("<http://ex.org/person1>", "<http://ex.org/hobby>", "\"hiking\""),
        ("<http://ex.org/person1>", "<http://ex.org/hobby>", "\"cooking\"")
      ).toDF("s", "p", "o")

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?person (GROUP_CONCAT(?hobby) AS ?hobbies)
        WHERE {
          ?person ex:hobby ?hobby
        }
        GROUP BY ?person
      """

      val result = Compiler.compile(df, query, config)

      result shouldBe a[Right[_, _]]
      // When implemented, verify concatenated string
    }

    "concatenate values with custom separator" ignore {
      val df = List(
        ("<http://ex.org/person1>", "<http://ex.org/tag>", "\"scala\""),
        ("<http://ex.org/person1>", "<http://ex.org/tag>", "\"spark\""),
        ("<http://ex.org/person1>", "<http://ex.org/tag>", "\"rdf\"")
      ).toDF("s", "p", "o")

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?person (GROUP_CONCAT(?tag; SEPARATOR=",") AS ?tags)
        WHERE {
          ?person ex:tag ?tag
        }
        GROUP BY ?person
      """

      val result = Compiler.compile(df, query, config)

      // When implemented, verify: "scala,spark,rdf"
    }
  }
}
```

### Example 4: Differential Testing

```scala
package com.gsk.kg.engine.compiler.integration

import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.jena.query.{QueryExecutionFactory, QueryFactory, ResultSetFactory}
import org.apache.jena.riot.RDFDataMgr
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.JavaConverters._

class DifferentialTestingSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "Bellman results" should {

    "match Apache Jena results for simple queries" in {
      val triples = List(
        ("<http://ex.org/s1>", "<http://ex.org/p1>", "\"value1\""),
        ("<http://ex.org/s2>", "<http://ex.org/p1>", "\"value2\"")
      )

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?s ?o
        WHERE {
          ?s ex:p1 ?o
        }
      """

      // Run in Bellman
      val df = triples.toDF("s", "p", "o")
      val bellmanResult = Compiler.compile(df, query, config)
        .right.get.collect.toSet

      // Run in Jena
      val model = createJenaModel(triples)
      val jenaQuery = QueryFactory.create(query)
      val qexec = QueryExecutionFactory.create(jenaQuery, model)
      val jenaResults = qexec.execSelect().asScala.toList

      // Compare (need to convert Jena results to comparable format)
      bellmanResult should have size jenaResults.size
    }
  }

  private def createJenaModel(triples: List[(String, String, String)]) = {
    val model = org.apache.jena.rdf.model.ModelFactory.createDefaultModel()
    triples.foreach { case (s, p, o) =>
      // Parse and add to model
      // Implementation details...
    }
    model
  }
}
```

---

## Performance and Regression Testing

### Performance Testing Strategy

```scala
package com.gsk.kg.engine.compiler.performance

import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class QueryPerformanceSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "query performance" should {

    "complete large dataset queries within acceptable time" in {
      // Generate large dataset
      val largeDF = (1 to 100000).flatMap { i =>
        List(
          (s"<http://ex.org/s$i>", "<http://ex.org/p1>", s"\"value$i\""),
          (s"<http://ex.org/s$i>", "<http://ex.org/p2>", s"\"data$i\"")
        )
      }.toDF("s", "p", "o")

      val query = """
        PREFIX ex: <http://ex.org/>
        SELECT ?s (COUNT(?o) AS ?count)
        WHERE {
          ?s ?p ?o
        }
        GROUP BY ?s
      """

      val startTime = System.currentTimeMillis()
      val result = Compiler.compile(largeDF, query, config)
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime

      result shouldBe a[Right[_, _]]
      result.right.get.count shouldEqual 100000

      // Performance assertion
      duration should be < 30000L  // Should complete in < 30 seconds
    }

    "optimize join order for better performance" in {
      // Test that optimizer reorders BGPs effectively
      // Measure query with and without optimization
    }
  }
}
```

### Regression Test Template

```scala
package com.gsk.kg.engine.compiler.regression

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Regression test for Issue #XXX
 *
 * Description: [Brief description of the bug]
 * Reporter: [GitHub username]
 * Date: [YYYY-MM-DD]
 *
 * Root Cause: [Brief explanation]
 * Fix: [Description of fix]
 */
class IssueXXXSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "Issue #XXX" should {

    "reproduce the bug" in {
      // Minimal reproduction case
      val df = List(
        // Test data that triggers the bug
      ).toDF("s", "p", "o")

      val query = """
        // Query that exposed the bug
      """

      val result = Compiler.compile(df, query, config)

      // Assert correct behavior (after fix)
      result shouldBe a[Right[_, _]]
      // Additional assertions
    }

    "not regress on edge case" in {
      // Additional test for related edge cases
    }
  }
}
```

---

## Continuous Integration

### Recommended CI Test Stages

```yaml
# .github/workflows/test.yml

name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up JDK
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run unit tests
        run: sbt "testOnly *.compiler.*Spec"
      - name: Generate coverage report
        run: sbt coverage test coverageReport
      - name: Upload coverage
        uses: codecov/codecov-action@v2

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up JDK
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run integration tests
        run: sbt "testOnly *.integration.*Spec"

  w3c-compliance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
        with:
          submodules: recursive  # For W3C test suite submodule
      - name: Set up JDK
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run W3C tests
        run: sbt "bellman-rdf-tests/test"
      - name: Generate test report
        run: |
          # Generate updated rdf-test-results.md
          # Fail if pass rate decreases

  performance-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up JDK
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run performance benchmarks
        run: sbt "testOnly *.performance.*Spec"
      - name: Compare with baseline
        run: |
          # Compare performance metrics with previous runs
          # Fail if significant regression detected
```

### Test Execution Commands

```bash
# Run all tests
sbt test

# Run specific test suite
sbt "testOnly com.gsk.kg.engine.compiler.GroupBySpec"

# Run tests matching pattern
sbt "testOnly *.compiler.*Spec"

# Run with coverage
sbt clean coverage test coverageReport

# Run only W3C tests
sbt "bellman-rdf-tests/test"

# Run tests and generate documentation
sbt ";test ;build-microsite"

# Run tests in continuous mode (rerun on file changes)
sbt ~test
```

---

## Prioritization Recommendations

### High Priority (Implement First)

1. **Fix W3C Compliance Tests**
   - Current pass rate: 3%
   - Goal: 50%+ pass rate
   - Focus on: Aggregates, BIND, VALUES
   - File: `modules/rdf-tests/src/test/scala/com/gsk/kg/RdfTests.scala`

2. **Add Integration Tests**
   - Create complex multi-feature tests
   - Real-world use case scenarios
   - Coverage for common query patterns

3. **Improve Error Testing**
   - Test malformed queries
   - Test type mismatches
   - Test edge cases that should fail gracefully

### Medium Priority

4. **Data Type Testing**
   - Comprehensive tests for all XSD datatypes
   - Type coercion and casting
   - Numeric operations across types

5. **Performance Benchmarks**
   - Large dataset tests
   - Query optimization validation
   - Regression detection

6. **Property Path Testing**
   - Currently only 3% passing
   - Critical for graph traversal queries

### Low Priority

7. **Differential Testing**
   - Compare with Jena/RDF4J
   - Useful for validation but time-intensive

8. **Fuzzing/Property-Based Testing**
   - Use ScalaCheck for property-based tests
   - Generate random valid SPARQL queries

---

## Summary and Next Steps

### Current State
- ✅ 98 feature test files covering individual SPARQL features
- ✅ W3C compliance test infrastructure in place
- ⚠️ Only 3% of W3C tests passing
- ❌ Limited integration testing
- ❌ Limited negative/error testing

### Recommended Next Steps

**Week 1-2: Improve W3C Compliance**
1. Analyze failing W3C tests in `rdf-test-results.md`
2. Group failures by category (aggregates, bindings, etc.)
3. Fix high-impact issues (e.g., BIND, VALUES)
4. Goal: 20% pass rate

**Week 3-4: Add Integration Tests**
1. Create `integration/` directory under test
2. Implement 10-15 complex query scenarios
3. Use realistic datasets and query patterns
4. Document expected results

**Week 5-6: Error Handling and Edge Cases**
1. Add negative tests for each feature
2. Test boundary conditions
3. Verify error messages are helpful
4. Goal: 100% code coverage for error paths

**Ongoing:**
- Add regression tests for each bug fix
- Monitor and improve test execution time
- Keep test documentation up to date
- Review and update this guide quarterly

---

## Resources

### Useful Links
- W3C SPARQL 1.1 Specification: https://www.w3.org/TR/sparql11-query/
- W3C Test Suite: https://www.w3.org/2009/sparql/docs/tests/
- ScalaTest Documentation: https://www.scalatest.org/
- Spark Testing Base: https://github.com/holdenk/spark-testing-base

### Internal References
- Existing tests: `/home/user/bellman/modules/engine/src/test/scala/`
- W3C tests: `/home/user/bellman/modules/rdf-tests/`
- Test results: `/home/user/bellman/rdf-test-results.md`
- Build file: `/home/user/bellman/build.sbt`

### Contact
For questions or suggestions about testing strategy, create an issue in the GitHub repository.

---

*Last Updated: 2025-11-09*
*Version: 1.0*

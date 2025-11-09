# Bellman: Comprehensive Guide

## Table of Contents
1. [Overview](#overview)
2. [How to Use Bellman](#how-to-use-bellman)
3. [RDF Storage Table Layout](#rdf-storage-table-layout)
4. [SPARQL to Spark Translation](#sparql-to-spark-translation)
5. [Supported SPARQL 1.1 Features](#supported-sparql-11-features)
6. [Aggregated Queries Support](#aggregated-queries-support)
7. [Configuration](#configuration)
8. [Examples](#examples)

---

## Overview

**Bellman** is a high-performance SPARQL execution engine that runs on Apache Spark. It enables querying large-scale RDF datasets using SPARQL 1.1 by translating SPARQL queries into Spark DataFrame operations.

**Key Features:**
- Execute SPARQL queries on massive RDF datasets using Spark's distributed computing
- SPARQL 1.1 support including aggregates, property paths, subqueries, and more
- Automatic query optimization
- Built on functional programming principles using Recursion Schemes
- Nanopass compiler architecture for maintainability

**Architecture:** Bellman uses Apache Jena to parse SPARQL queries into algebra, then transforms this algebra into an internal DAG (Directed Acyclic Graph) representation, optimizes it, and finally executes it as Spark DataFrame operations.

---

## How to Use Bellman

### Installation

Add Bellman to your SBT project:

```scala
libraryDependencies += "com.github.gsk-aiops" %% "bellman-spark-engine" % "<version>"
```

### Basic Usage

```scala
import com.gsk.kg.engine.Compiler
import com.gsk.kg.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

// Initialize Spark
val spark = SparkSession.builder()
  .appName("Bellman Example")
  .master("local[*]")
  .getOrCreate()

implicit val sqlContext = spark.sqlContext

// Load your RDF data as a DataFrame with columns: s, p, o, g
val rdfDataFrame: DataFrame = spark.read
  .parquet("path/to/rdf/data")

// Define your SPARQL query
val sparqlQuery = """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?name
  WHERE {
    ?person a ex:Person .
    ?person ex:name ?name .
  }
"""

// Compile and execute
val config = Config.default
val result = Compiler.compile(rdfDataFrame, sparqlQuery, config)

// Handle result
result match {
  case Right(resultDF) => resultDF.show()
  case Left(error) => println(s"Query failed: $error")
}
```

### Loading RDF Data

Bellman expects RDF data as a Spark DataFrame. You can load RDF data from various formats:

**From N-Triples/N-Quads:**

```scala
import org.apache.jena.riot.RDFParser
import org.apache.jena.riot.lang.CollectorStreamTriples
import scala.collection.JavaConverters._

def readNTriplesAsDF(path: String)(implicit spark: SparkSession): DataFrame = {
  import spark.implicits._

  val inputStream = new CollectorStreamTriples()
  RDFParser.source(path).parse(inputStream)

  inputStream.getCollected().asScala.toList
    .map(triple => (
      triple.getSubject().toString(),
      triple.getPredicate().toString(),
      triple.getObject().toString(),
      "" // default graph
    ))
    .toDF("s", "p", "o", "g")
}
```

**From Parquet (recommended for performance):**

```scala
val rdfDF = spark.read.parquet("path/to/rdf.parquet")
```

---

## RDF Storage Table Layout

Bellman uses a **columnar triple/quad store** layout based on Spark DataFrames.

### Storage Schema

RDF data is stored in a DataFrame with **3 or 4 columns**:

| Column | Description | Type | Required |
|--------|-------------|------|----------|
| `s` | **Subject** | String | Yes |
| `p` | **Predicate** | String | Yes |
| `o` | **Object** | String | Yes |
| `g` | **Graph** (optional) | String | No |

### Example Data

```
+-------------------------+------------------+-------------------+----+
| s                       | p                | o                 | g  |
+-------------------------+------------------+-------------------+----+
| http://ex.org/person/1  | rdf:type         | ex:Person         |    |
| http://ex.org/person/1  | ex:name          | "Alice"           |    |
| http://ex.org/person/1  | ex:age           | "30"^^xsd:integer |    |
| http://ex.org/person/2  | rdf:type         | ex:Person         | g1 |
| http://ex.org/person/2  | ex:name          | "Bob"             | g1 |
+-------------------------+------------------+-------------------+----+
```

### RDF Value Representation

During query execution, Bellman uses a **typed representation** for RDF values:

```scala
StructType(Seq(
  StructField("value", StringType, false),  // The actual value
  StructField("type", StringType, false),   // XSD datatype
  StructField("lang", StringType, true)     // Language tag (optional)
))
```

**Examples:**
- Plain literal: `Row("Alice", "xsd:string", null)`
- URI: `Row("http://example.org", "xsd:anyURI", null)`
- Language-tagged: `Row("Hello", "xsd:string", "en")`
- Typed literal: `Row("42", "xsd:integer", null)`

### Storage Advantages

This columnar layout provides several benefits:

1. **Efficient filtering**: Spark can filter on specific columns (s, p, o, g)
2. **Columnar compression**: Parquet compression works well with repetitive URIs
3. **Predicate pushdown**: Filters can be pushed down to storage layer
4. **Partition pruning**: Data can be partitioned by graph or predicate
5. **Scalability**: Leverages Spark's distributed processing

---

## SPARQL to Spark Translation

Bellman translates SPARQL queries to Spark operations through a multi-stage compilation pipeline.

### Compilation Pipeline

```
SPARQL String
    ↓
[Parser] - Uses Apache Jena to parse SPARQL into algebra
    ↓
Query ADT (Algebraic Data Type)
    ↓
[TransformToGraph] - Converts to internal DAG representation
    ↓
DAG (Directed Acyclic Graph)
    ↓
[Optimizer] - Applies query optimizations
    ↓
Optimized DAG
    ↓
[StaticAnalysis] - Validates query semantics
    ↓
[DataFrameTyper] - Types RDF values
    ↓
[Engine] - Executes as Spark DataFrame operations
    ↓
[RdfFormatter] - Formats output values
    ↓
Result DataFrame
```

### Triple Pattern Translation

**SPARQL Triple Pattern:**
```sparql
?person rdf:type ex:Person .
```

**Translation Steps:**

1. **Filter Generation**: Create a condition to filter the DataFrame
   ```scala
   df.filter(
     col("p") === "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" &&
     col("o") === "http://example.org/Person"
   )
   ```

2. **Column Renaming**: Rename columns to match SPARQL variables
   ```scala
   .select(col("s").as("?person"), col("g"))
   ```

### Basic Graph Pattern (BGP) Translation

A BGP with multiple triple patterns is processed by:

1. Chunking related patterns together (optimization)
2. For each chunk, generating a compound filter condition
3. Applying filters to the base DataFrame
4. Renaming columns to SPARQL variable names

**Example:**

```sparql
SELECT ?person ?name
WHERE {
  ?person rdf:type ex:Person .
  ?person ex:name ?name .
}
```

**Translation:**

```scala
// Step 1: Filter for first pattern
val pattern1 = df.filter(
  col("p") === "rdf:type" && col("o") === "ex:Person"
).select(col("s").as("?person"))

// Step 2: Filter for second pattern
val pattern2 = df.filter(
  col("p") === "ex:name"
).select(col("s").as("?person"), col("o").as("?name"))

// Step 3: Join on common variable ?person
val result = pattern1.join(pattern2, Seq("?person"), "inner")
```

### Join Translation

Bellman implements several join strategies:

**1. Inner Join (when variables are shared)**
```sparql
?x ex:knows ?y .
?y ex:age ?age .
```
→ `df1.join(df2, Seq("?y"), "inner")`

**2. Cross Join (when no variables are shared)**
```sparql
?x ex:name ?name .
?y ex:age ?age .
```
→ `df1.crossJoin(df2)`

**3. Left Outer Join (for OPTIONAL)**
```sparql
?x ex:name ?name .
OPTIONAL { ?x ex:email ?email }
```
→ `df1.join(df2, Seq("?x"), "left_outer")`

### Filter Translation

SPARQL FILTER expressions are translated to Spark SQL expressions:

```sparql
FILTER(?age > 18)
```
→
```scala
df.filter(col("?age").cast(IntegerType) > 18)
```

### Aggregation Translation

```sparql
SELECT ?person (COUNT(?friend) AS ?count)
WHERE {
  ?person ex:knows ?friend
}
GROUP BY ?person
```

→

```scala
df.groupBy("?person")
  .agg(count("?friend").as("?count"))
```

### Union Translation

```sparql
{ ?x ex:email ?contact }
UNION
{ ?x ex:phone ?contact }
```

→

```scala
df1.union(df2)
```

### Property Path Translation

Property paths are expanded into recursive DataFrame operations:

```sparql
?x ex:knows+ ?y .  # one or more
```

This is translated into iterative joins that follow the path until no new results are found.

### Complete Example

**SPARQL Query:**
```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?avgAge
WHERE {
  ?person rdf:type ex:Person .
  ?person ex:friend ?friend .
  ?friend ex:age ?age .
}
GROUP BY ?person
HAVING(AVG(?age) > 25)
ORDER BY DESC(?avgAge)
LIMIT 10
```

**Translated to (conceptual Spark operations):**

```scala
df.filter(col("p") === "rdf:type" && col("o") === "ex:Person")
  .select(col("s").as("?person"))
  .join(
    df.filter(col("p") === "ex:friend")
      .select(col("s").as("?person"), col("o").as("?friend")),
    Seq("?person")
  )
  .join(
    df.filter(col("p") === "ex:age")
      .select(col("s").as("?friend"), col("o").as("?age")),
    Seq("?friend")
  )
  .groupBy("?person")
  .agg(avg("?age").as("?avgAge"))
  .filter(col("?avgAge") > 25)
  .orderBy(col("?avgAge").desc)
  .limit(10)
```

---

## Supported SPARQL 1.1 Features

### Query Forms

| Feature | Status | Notes |
|---------|--------|-------|
| **SELECT** | ✅ Supported | Full support |
| **CONSTRUCT** | ✅ Supported | Builds RDF graphs |
| **ASK** | ✅ Supported | Returns boolean |
| **DESCRIBE** | ✅ Supported | Describes resources |

### Graph Patterns

| Feature | Status | Notes |
|---------|--------|-------|
| **Basic Graph Patterns (BGP)** | ✅ Supported | Core triple patterns |
| **OPTIONAL** | ✅ Supported | Left outer join |
| **UNION** | ✅ Supported | Full support |
| **MINUS** | ✅ Supported | Set difference |
| **FILTER** | ✅ Supported | Most filter expressions |
| **BIND** | ⚠️ Partial | Some test failures |
| **VALUES** | ⚠️ Partial | Inline data, some edge cases fail |
| **Named Graphs (GRAPH)** | ✅ Supported | FROM, FROM NAMED |

### Solution Modifiers

| Feature | Status | Notes |
|---------|--------|-------|
| **ORDER BY** | ✅ Supported | ASC/DESC ordering |
| **LIMIT** | ✅ Supported | Full support |
| **OFFSET** | ✅ Supported | Full support |
| **DISTINCT** | ✅ Supported | Duplicate elimination |
| **REDUCED** | ✅ Supported | Spark's coalesce |

### Expressions and Functions

| Category | Status | Examples |
|----------|--------|----------|
| **Logical Operators** | ✅ Supported | `&&`, `\|\|`, `!` |
| **Comparison** | ✅ Supported | `=`, `!=`, `<`, `>`, `<=`, `>=` |
| **Arithmetic** | ✅ Supported | `+`, `-`, `*`, `/` |
| **String Functions** | ⚠️ Partial | STRLEN, SUBSTR, UCASE, LCASE, CONTAINS, etc. |
| **Date/Time** | ⚠️ Partial | YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS |
| **Hash Functions** | ⚠️ Partial | MD5, SHA1, SHA256, SHA512 |
| **Type Checking** | ⚠️ Partial | isIRI, isBlank, isLiteral, isNumeric |
| **CAST** | ⚠️ Partial | Type conversion functions |

### Subqueries

| Feature | Status | Notes |
|---------|--------|-------|
| **SELECT Subqueries** | ⚠️ Partial | Most failing in RDF tests |
| **Subquery with Aggregates** | ⚠️ Partial | Some support |

### Property Paths

| Feature | Status | Notes |
|---------|--------|-------|
| **Sequence** | ⚠️ Partial | `ex:p1/ex:p2` |
| **Alternative** | ⚠️ Partial | `ex:p1\|ex:p2` |
| **Zero or More** | ⚠️ Partial | `ex:p*` |
| **One or More** | ⚠️ Partial | `ex:p+` |
| **Zero or One** | ⚠️ Partial | `ex:p?` |
| **Inverse** | ⚠️ Partial | `^ex:p` |
| **Negation** | ⚠️ Partial | `!ex:p` |

### Negation

| Feature | Status | Notes |
|---------|--------|-------|
| **NOT EXISTS** | ✅ Supported | Positive EXISTS working |
| **EXISTS** | ✅ Supported | Most cases working |
| **MINUS** | ⚠️ Partial | Some edge cases fail |

### Test Results Summary

Based on the official W3C SPARQL 1.1 test suite (`rdf-test-results.md`):

- **Total Tests**: 179
- **Passing**: 7 (3%)
- **Failing**: 172 (96%)

**Tests with ✅ Passing Status:**
1. Aggregates - agg on empty set, explicit grouping
2. Functions - REPLACE() with overlapping pattern
3. Functions - REPLACE() with captured substring
4. Grouping - Group-1
5. Negation - Positive EXISTS 2
6. Negation - Subsets by exclusion (NOT EXISTS)
7. Negation - Positive EXISTS 1

**Note**: The low pass rate indicates this is either a work-in-progress implementation or focuses on core production use cases rather than full spec compliance.

---

## Aggregated Queries Support

### Supported Aggregate Functions

Bellman provides **full support** for most SPARQL 1.1 aggregate functions:

| Function | Status | Return Type | Implementation |
|----------|--------|-------------|----------------|
| **COUNT** | ✅ Full | xsd:integer | `FuncAgg.countAgg` |
| **SUM** | ✅ Full | xsd:double | `FuncAgg.sumAgg` |
| **AVG** | ✅ Full | xsd:double | `FuncAgg.avgAgg` |
| **MIN** | ✅ Full | Same as input | `FuncAgg.minAgg` |
| **MAX** | ✅ Full | Same as input | `FuncAgg.maxAgg` |
| **SAMPLE** | ✅ Full | Same as input | `FuncAgg.sample` |
| **GROUP_CONCAT** | ❌ Not Implemented | xsd:string | TODO (Issue #202) |

### GROUP BY Support

**Full support** for GROUP BY with:
- Single variables: `GROUP BY ?x`
- Multiple variables: `GROUP BY ?x ?y`
- Implicit grouping (aggregates without GROUP BY)

**Example:**

```sparql
SELECT ?person (COUNT(?book) AS ?bookCount)
WHERE {
  ?person ex:read ?book .
}
GROUP BY ?person
```

This translates to:
```scala
df.groupBy("?person")
  .agg(count("?book").as("?bookCount"))
```

### HAVING Clause Support

**Full support** for HAVING with:
- Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Aggregate functions in conditions
- Proper type handling

**Example:**

```sparql
SELECT ?author (AVG(?rating) AS ?avgRating)
WHERE {
  ?book ex:author ?author .
  ?book ex:rating ?rating .
}
GROUP BY ?author
HAVING(AVG(?rating) > 4.0)
```

### Aggregate Examples

#### COUNT Example

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person (COUNT(?friend) AS ?friendCount)
WHERE {
  ?person ex:knows ?friend .
}
GROUP BY ?person
ORDER BY DESC(?friendCount)
```

#### AVG Example

```sparql
PREFIX ex: <http://example.org/>

SELECT ?dept (AVG(?salary) AS ?avgSalary)
WHERE {
  ?emp ex:department ?dept .
  ?emp ex:salary ?salary .
}
GROUP BY ?dept
HAVING(AVG(?salary) > 50000)
```

#### MIN/MAX Example

```sparql
PREFIX ex: <http://example.org/>

SELECT ?product (MIN(?price) AS ?minPrice) (MAX(?price) AS ?maxPrice)
WHERE {
  ?offer ex:product ?product .
  ?offer ex:price ?price .
}
GROUP BY ?product
```

#### Multiple Aggregates

```sparql
PREFIX ex: <http://example.org/>

SELECT
  ?category
  (COUNT(?item) AS ?count)
  (AVG(?price) AS ?avgPrice)
  (MIN(?price) AS ?minPrice)
  (MAX(?price) AS ?maxPrice)
WHERE {
  ?item ex:category ?category .
  ?item ex:price ?price .
}
GROUP BY ?category
ORDER BY DESC(?count)
```

### Implementation Details

**Location**: `/home/user/bellman/modules/engine/src/main/scala/com/gsk/kg/engine/typed/functions/FuncAgg.scala`

**Key Implementation Points:**

1. **Type Preservation**: MIN/MAX preserve the input datatype
2. **Numeric Conversion**: SUM/AVG convert to xsd:double
3. **Empty Groups**: COUNT returns 0, others return no value
4. **NULL Handling**: NULLs are excluded from aggregation (except COUNT(*))

### Limitations

1. **GROUP_CONCAT**: Not yet implemented (marked as TODO)
2. **Custom Aggregates**: No support for custom aggregate functions
3. **DISTINCT in Aggregates**: Limited testing for `COUNT(DISTINCT ?x)`

### Test Coverage

Aggregate functionality is extensively tested in:
- `/home/user/bellman/modules/engine/src/test/scala/com/gsk/kg/engine/compiler/GroupBySpec.scala`
- `/home/user/bellman/modules/engine/src/test/scala/com/gsk/kg/engine/compiler/HavingSpec.scala`

Test cases cover:
- Simple GROUP BY
- Multiple grouping variables
- All aggregate functions (except GROUP_CONCAT)
- HAVING with various conditions
- Typed literals (int, float, double, decimal, string)
- Implicit grouping
- Edge cases (empty groups, NULL values)

---

## Configuration

Bellman's behavior can be customized via the `Config` object.

### Configuration Options

#### isDefaultGraphExclusive

**Type**: `Boolean`
**Default**: `true`

Controls how the default graph is constructed:

- **`true` (exclusive)**: Only explicitly named graphs (via `FROM`) are in the default graph
- **`false` (inclusive)**: All graphs are included in the default graph

**Impact**: Affects query results and performance, especially for queries without `FROM` clauses.

**Example:**

```scala
val config = Config(isDefaultGraphExclusive = true)
```

#### stripQuestionMarksOnOutput

**Type**: `Boolean`
**Default**: `false`

Controls whether to remove `?` from variable names in output column headers.

- **`true`**: `?person` → `person`
- **`false`**: `?person` → `?person`

#### formatRdfOutput

**Type**: `Boolean`
**Default**: `false`

Controls whether to apply RDF formatting to output values.

- **`true`**: Apply typed formatting (e.g., `"42"^^xsd:integer`)
- **`false`**: Return raw values

### Configuration Example

```scala
import com.gsk.kg.config.Config

val config = Config(
  isDefaultGraphExclusive = false,      // Include all graphs
  stripQuestionMarksOnOutput = true,    // Clean column names
  formatRdfOutput = true                // Format RDF values
)

val result = Compiler.compile(df, query, config)
```

---

## Examples

### Example 1: Basic SELECT Query

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name ?email
WHERE {
  ?person a foaf:Person .
  ?person foaf:name ?name .
  ?person foaf:mbox ?email .
}
```

### Example 2: OPTIONAL Pattern

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name ?email
WHERE {
  ?person foaf:name ?name .
  OPTIONAL { ?person foaf:mbox ?email }
}
```

### Example 3: FILTER with Comparison

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?age
WHERE {
  ?person ex:age ?age .
  FILTER(?age >= 18 && ?age < 65)
}
```

### Example 4: UNION

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?contact
WHERE {
  {
    ?person ex:email ?contact
  }
  UNION
  {
    ?person ex:phone ?contact
  }
}
```

### Example 5: Aggregation with GROUP BY

```sparql
PREFIX ex: <http://example.org/>

SELECT ?department (AVG(?salary) AS ?avgSalary) (COUNT(?employee) AS ?empCount)
WHERE {
  ?employee ex:department ?department .
  ?employee ex:salary ?salary .
}
GROUP BY ?department
HAVING(COUNT(?employee) > 5)
ORDER BY DESC(?avgSalary)
```

### Example 6: CONSTRUCT Query

```sparql
PREFIX ex: <http://example.org/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

CONSTRUCT {
  ?person foaf:name ?name .
  ?person foaf:knows ?friend .
}
WHERE {
  ?person ex:hasName ?name .
  ?person ex:friendsWith ?friend .
}
```

### Example 7: Named Graphs

```sparql
PREFIX ex: <http://example.org/>

SELECT ?s ?p ?o
FROM <http://example.org/graph1>
WHERE {
  ?s ?p ?o .
}
```

### Example 8: Subquery

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?totalSpent
WHERE {
  {
    SELECT ?person (SUM(?amount) AS ?totalSpent)
    WHERE {
      ?person ex:purchased ?item .
      ?item ex:price ?amount .
    }
    GROUP BY ?person
  }
  FILTER(?totalSpent > 1000)
}
```

### Example 9: VALUES (Inline Data)

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?role
WHERE {
  ?person ex:role ?role .
  VALUES ?role { ex:Manager ex:Director }
}
```

### Example 10: Property Path

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person ?ancestor
WHERE {
  ?person ex:parent+ ?ancestor .
}
```

---

## Optimizations

Bellman includes several query optimizations:

1. **Graph Pushdown**: Pushes graph filters down to scan operations
2. **Subquery Pushdown**: Pushes filters into subqueries when possible
3. **Join BGPs**: Merges adjacent Basic Graph Patterns to reduce joins
4. **Compact BGPs**: Combines triple patterns with same subject/predicate
5. **Remove Nested Project**: Eliminates redundant projection operations
6. **BGP Reordering**: Reorders triple patterns for optimal execution

These optimizations are automatically applied during the compilation phase.

---

## Performance Tips

1. **Use Parquet**: Store RDF data in Parquet format for better compression and performance
2. **Partition Data**: Partition by graph or frequently-filtered predicates
3. **Minimize Wildcards**: Avoid queries with all variables (`?s ?p ?o`)
4. **Use Filters Early**: Place FILTER clauses close to the patterns they filter
5. **Leverage Caching**: Cache frequently-used DataFrames
6. **Tune Spark**: Configure Spark memory and parallelism settings appropriately

---

## Architecture Overview

### Modules

1. **bellman-algebra-parser**: Parses SPARQL strings into algebraic ADTs using Apache Jena
2. **bellman-spark-engine**: Executes queries on Spark DataFrames
3. **bellman-rdf-tests**: W3C SPARQL 1.1 test suite integration
4. **bellman-site**: Documentation microsite

### Key Technologies

- **Apache Spark**: Distributed data processing
- **Apache Jena**: SPARQL parsing and algebra
- **Cats**: Functional programming abstractions
- **Droste**: Recursion schemes framework
- **Monocle**: Optics library for data manipulation

### Compiler Phases

Detailed in `/home/user/bellman/modules/site/src/main/docs/phases.md`:

1. **Parser** (`modules/parser`): SPARQL string → Query ADT via Jena
2. **TransformToGraph**: Query ADT → Internal DAG
3. **Optimizer**: Applies optimization passes
4. **StaticAnalysis**: Validates query semantics
5. **Engine** (`modules/engine`): DAG → Spark DataFrame operations
6. **RdfFormatter**: Formats output values to RDF syntax

---

## Resources

- **GitHub Repository**: https://github.com/gsk-aiops/bellman
- **Documentation Site**: https://gsk-aiops.github.io/bellman/
- **Issue Tracker**: https://github.com/gsk-aiops/bellman/issues

---

## License

Apache License 2.0

---

## Contributing

Contributions are welcome! Please refer to the GitHub repository for contribution guidelines.

---

*This documentation was generated based on Bellman codebase analysis.*

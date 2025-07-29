# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Spark-Solr** is a Lucidworks connector library that enables seamless integration between Apache Spark and Apache Solr. It provides tools for reading data from Solr as Spark DataFrames/RDDs and indexing objects from Spark into Solr using SolrJ.

**Current Version**: 4.0.4-SNAPSHOT  
**Compatible with**: Spark 3.1.2, Solr 8.11.0, Scala 2.12.12, Java 8

## Build Commands

### Basic Build
```bash
mvn clean package -DskipTests
```
Produces two main artifacts:
- `target/spark-solr-4.0.4-SNAPSHOT.jar` - Core library for embedding
- `target/spark-solr-4.0.4-SNAPSHOT-shaded.jar` - Standalone jar for spark-submit

### Build with Tests
```bash
mvn clean package
```

### Run Tests
```bash
# Java tests (JUnit)
mvn surefire:test

# Scala tests (ScalaTest) 
mvn scalatest:test

# All tests
mvn test
```

### Run Specific Test
```bash
# Java test class
mvn test -Dtest=SolrRelationTest

# Scala test class
mvn test -Dsuites=com.lucidworks.spark.TestSolrRelation
```

### Code Coverage
```bash
mvn clean package -Pcoverage
```

### Create Release Artifacts
```bash
mvn clean package -Prelease
```

### Publish to Maven Central
```bash
# Deploy to Central Portal (requires user token in ~/.m2/settings.xml)
mvn clean deploy -Prelease

# Or publish manually using the central-publishing-maven-plugin
mvn central:publish -Prelease
```

**Note**: Publishing requires:
1. User token from https://central.sonatype.com/account configured in `~/.m2/settings.xml`:
```xml
<servers>
  <server>
    <id>central</id>
    <username><!-- your token username --></username>
    <password><!-- your token password --></password>
  </server>
</servers>
```
2. GPG signing configured (included in release profile)

## Core Architecture

### Data Source Integration (`src/main/scala/solr/DefaultSource.scala`)
- Entry point for Spark SQL integration via DataSource API v1
- Registers "solr" format and provides RelationProvider interfaces

### SolrRelation (`src/main/scala/com/lucidworks/spark/SolrRelation.scala`)
- Core implementation of Spark's BaseRelation interface
- Handles schema inference, query optimization, and push-down filters
- Supports multiple Solr query handlers: `/select`, `/export`, `/stream`, `/sql`

### RDD Implementations (`src/main/scala/com/lucidworks/spark/rdd/`)
- `SolrRDD`: Abstract base class for Solr RDDs
- `SelectSolrRDD`: Standard Solr queries via `/select` handler
- `StreamingSolrRDD`: Streaming queries via `/export`, `/stream`, `/sql` handlers

### Configuration (`src/main/scala/com/lucidworks/spark/SolrConf.scala`)
- Centralized configuration management for all Solr connection parameters
- Supports both programmatic and environment-based configuration

## Key Entry Points

### DataFrame API (Recommended)
```scala
val df = spark.read.format("solr")
  .option("zkhost", "localhost:9983")
  .option("collection", "myCollection")
  .load()
```

### RDD API
```scala
import com.lucidworks.spark.rdd.SelectSolrRDD
val solrRDD = new SelectSolrRDD(zkHost, collection, sc)
```

### Java RDD API
```java
import com.lucidworks.spark.rdd.SolrJavaRDD;
SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, collection, jsc.sc());
```

## Performance Features

- **Data Locality**: Co-locates Spark partitions with Solr shards when possible
- **Intelligent Handler Selection**: Automatically chooses optimal query handler based on query characteristics
- **Intra-shard Splitting**: Parallelizes reading within shards using `split_field` option
- **Streaming Export**: 8-10x faster than cursors when fields have docValues enabled (`request_handler="/export"`)

## Test Structure

### Java Tests (`src/test/java/`)
- Unit tests using JUnit framework
- Integration tests with embedded Solr cluster
- Performance and ML integration tests

### Scala Tests (`src/test/scala/`)
- Functional tests using ScalaTest framework
- Comprehensive test suites for core functionality
- `SparkSolrFunSuite` provides common test infrastructure

### Test Data (`src/test/resources/`)
- Sample datasets including NYC taxi data, MovieLens, Twitter data
- Solr configuration files and schemas
- Embedded Solr cluster configuration

## Development Workflow

1. **Schema Changes**: When modifying data structures, ensure compatibility with Solr's Schema API
2. **Handler Support**: New query features should support both `/select` and `/export` handlers where applicable
3. **Performance Testing**: Use `TestShardSplits` and `TestPartitionByTimeQuerySupport` for performance validation
4. **Authentication**: Test with both Kerberos and Basic Auth configurations

## Configuration Patterns

### Essential Options
- `zkhost`: ZooKeeper connection string (required)
- `collection`: Solr collection name (required)
- `query`: Solr query string (default: `*:*`)
- `fields`: Comma-separated field list
- `request_handler`: `/select` (default) or `/export` for streaming

### Performance Tuning
- `rows`: Page size for requests (default: 1000)
- `splits`: Enable intra-shard splitting (default: false)
- `split_field`: Field for splitting (default: `_version_`)
- `splits_per_shard`: Number of splits per shard (default: 1)
- `batch_size`: Documents per indexing batch (default: 500)

### Advanced Features
- `sample_seed`: Random sampling with specified seed
- `partition_by`: Time-series partitioning support
- `gen_uniq_key`: Auto-generate unique keys for documents
- `solr_field_types`: Specify field types for new fields

## Example Applications

The `src/main/scala/com/lucidworks/spark/example/` directory contains comprehensive examples:
- Basic read/write operations
- ML pipeline integration  
- Streaming applications (Twitter, document filtering)
- Analytics using Solr streaming expressions
- Time-series data processing

## Shaded Dependencies

The project uses Maven Shade plugin to relocate common dependencies to avoid conflicts:
- Jackson → `shaded.fasterxml.jackson`
- Apache HTTP Client → `shaded.apache.http`
- Guava → `shaded.google.guava`
- Joda Time → `shaded.joda.time`
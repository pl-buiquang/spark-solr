package com.lucidworks.spark.util

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.lucidworks.spark.filter.DocFilterContext
import com.lucidworks.spark.LazyLogging
import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Document filtering utilities that use embedded Solr for advanced streaming operations.
 * This is separated from SolrSupport to avoid serialization issues with regular Spark operations.
 */
object DocumentFilterSupport extends LazyLogging {

  def filterDocuments(
      filterContext: DocFilterContext,
      zkHost: String,
      collection: String,
      docs: DStream[SolrInputDocument]): DStream[SolrInputDocument] = {
    val partitionIndex = new AtomicInteger(0)
    val idFieldName = filterContext.getDocIdFieldName

    docs.mapPartitions(solrInputDocumentIterator => {
      val startNano: Long = System.nanoTime()
      val partitionId: Int = partitionIndex.incrementAndGet()

      val partitionFq: String = "docfilterid_i:" + partitionId
      // TODO: Can this be used concurrently? probably better to have each partition check it out from a pool
      val solr = EmbeddedSolrServerFactory.singleton.getEmbeddedSolrServer(zkHost, collection)

      // index all docs in this partition, then match queries
      var numDocs: Int = 0
      val inputDocs: mutable.Map[String, SolrInputDocument] = new mutable.HashMap[String, SolrInputDocument]
      while (solrInputDocumentIterator.hasNext) {
        numDocs += 1
        val doc: SolrInputDocument = solrInputDocumentIterator.next()
        doc.setField("docfilterid_i", partitionId)
        solr.add(doc)
        inputDocs.put(doc.getFieldValue(idFieldName).asInstanceOf[String], doc)
      }
      solr.commit

      for (q: SolrQuery <- filterContext.getQueries.asScala) {
        val query = q.getCopy
        query.setFields(idFieldName)
        query.setRows(inputDocs.size)
        query.addFilterQuery(partitionFq)

        var queryResponse: Option[QueryResponse] = None
        try {
          queryResponse = Some(solr.query(query))
        }
        catch {
          case e: SolrServerException =>
            throw new RuntimeException(e)
        }

        if (queryResponse.isDefined) {
          for (doc: SolrDocument  <- queryResponse.get.getResults.asScala) {
            val docId: String = doc.getFirstValue(idFieldName).asInstanceOf[String]
            val inputDoc = inputDocs.get(docId)
            if (inputDoc.isDefined) filterContext.onMatch(q, inputDoc.get)
          }

          solr.deleteByQuery(partitionFq, 100)
          val durationNano: Long = System.nanoTime - startNano

          logger.debug("Partition " + partitionId + " took " + TimeUnit.MILLISECONDS.convert(durationNano, TimeUnit.NANOSECONDS) + "ms to process " + numDocs + " docs")
          for (inputDoc <- inputDocs.values) {
            inputDoc.removeField("docfilterid_i")
          }
        }
      }

      inputDocs.valuesIterator
    })
  }
}
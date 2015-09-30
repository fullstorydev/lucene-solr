/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.RTimer;

public class FullJoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "fulljoin";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fromField = getParam("from");
        String toField = getParam("to");
        String v = localParams.get("v");
        QParser fromQueryParser = subQuery(v, null);
        Query fromQuery = fromQueryParser.getQuery();

        return new FullJoinQuery(fromField, toField, fromQuery, v);
      }
    };
  }
}


class FullJoinQuery extends Query {
  String fromField;
  String toField;
  Query q;
  String v;

  public FullJoinQuery(String fromField, String toField, Query subQuery, String v) {
    this.fromField = fromField;
    this.toField = toField;
    this.q = subQuery;
    this.v = v;
  }

  public Query getQuery() { return q; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // don't rewrite the subQuery
    return super.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new FullJoinQueryWeight((SolrIndexSearcher) searcher);
  }

  private class FullJoinQueryWeight extends ConstantScoreWeight {
    SolrIndexSearcher searcher;

    public FullJoinQueryWeight(SolrIndexSearcher searcher) {
      super(FullJoinQuery.this);
      this.searcher = searcher;
    }

    DocSet resultSet;
    Filter filter;


    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (filter == null) {
        ResponseBuilder rb = null;
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if (info != null) {
          rb = info.getResponseBuilder();
          SolrQueryRequest req = info.getReq();
          if (req != null) {
            boolean warming = req.getParams().getBool("warming", false);
            if (warming) {
              // Do not try to process warming queries at all, you can't get the right facet results.
              throw new ExitableDirectoryReader.ExitingReaderException("Refusing to autowarm fulljoin");
            }
          }
        }

        boolean debug = rb != null && rb.isDebug();
        RTimer timer = (debug ? new RTimer() : null);
        resultSet = getDocSet();
        if (timer != null) timer.stop();

        if (debug) {
          SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
          dbg.add("time", (long) timer.getTime());
          dbg.add("fromSetSize", fromSetSize);  // the input
          dbg.add("toSetSize", resultSet.size());    // the output

          dbg.add("fromTermCount", fromTermCount);
          dbg.add("fromTermTotalDf", fromTermTotalDf);
          dbg.add("fromTermDirectCount", fromTermDirectCount);
          dbg.add("fromTermHits", fromTermHits);
          dbg.add("fromTermHitsTotalDf", fromTermHitsTotalDf);
          dbg.add("toTermHits", toTermHits);
          dbg.add("toTermHitsTotalDf", toTermHitsTotalDf);
          dbg.add("toTermDirectCount", toTermDirectCount);
          dbg.add("smallSetsDeferred", smallSetsDeferred);
          dbg.add("toSetDocsAdded", resultListDocs);

          // TODO: perhaps synchronize  addDebug in the future...
          rb.addDebug(dbg, "fulljoin", FullJoinQuery.this.toString());
        }

        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, null);
      if (readerSet == null) {
        return null;
      }
      DocIdSetIterator readerSetIterator = readerSet.iterator();
      if (readerSetIterator == null) {
        return null;
      }
      return new ConstantScoreScorer(this, score(), readerSetIterator);
    }


    int fromSetSize;          // number of docs in the fromSet (that match the from query)
    long resultListDocs;      // total number of docs collected
    int fromTermCount;
    long fromTermTotalDf;
    int fromTermDirectCount;  // number of fromTerms that were too small to use the filter cache
    int fromTermHits;         // number of fromTerms that intersected the from query
    long fromTermHitsTotalDf; // sum of the df of the matching terms
    int toTermHits;           // num if intersecting from terms that match a term in the to field
    long toTermHitsTotalDf;   // sum of the df for the toTermHits
    int toTermDirectCount;    // number of toTerms that we set directly on a bitset rather than doing set intersections
    int smallSetsDeferred;    // number of small sets collected to be used later to intersect w/ bitset or create another small set


    private List<FacetField.Count> facet(String fromField) throws IOException {
      ModifiableSolrParams q = new ModifiableSolrParams();
      q.add("q", "*:*");
      q.add("fl", fromField);
      q.add("facet", "true");
      q.add("facet.field", fromField);
      q.add("facet.mincount", "1");
      q.add("facet.limit", "-1");
      q.add("facet.sort", "index");
      q.add("rows", "0");

      if (searcher.getCore().getCoreDescriptor().getCloudDescriptor() == null) {
        // Use the parsed query; for some reason the original query doesn't work locally.
        q.add("fq", FullJoinQuery.this.q.toString());
        try(LocalSolrQueryRequest r = new LocalSolrQueryRequest(searcher.getCore(), q)) {
          SolrQueryResponse rsp = new SolrQueryResponse();
          searcher.getCore().getRequestHandler("").handleRequest(r, rsp);
          ResultContext response = (ResultContext) rsp.getValues().get("response");
          fromSetSize = response.docs.matches();

          List<FacetField.Count> result = new ArrayList<FacetField.Count>();
          SimpleOrderedMap fcMap = (SimpleOrderedMap) rsp.getValues().get("facet_counts");
          SimpleOrderedMap ffMap = (SimpleOrderedMap) fcMap.get("facet_fields");
          NamedList fieldMap = (NamedList) ffMap.get(fromField);

          Iterator<Map.Entry> iterator = fieldMap.iterator();
          while (iterator.hasNext()) {
            Map.Entry entry = iterator.next();
            result.add(new FacetField.Count(null, (String) entry.getKey(), (Integer) entry.getValue()));
          }
          return result;
        }
      } else {
        // Just use the original query.
        q.add("fq", FullJoinQuery.this.v);
        String collectionName = searcher.getCore().getCoreDescriptor().getCollectionName();
        ZkController zkController = searcher.getCore().getCoreDescriptor().getCoreContainer().getZkController();
        String fromCollectionUrl = zkController.getBaseUrl() + "/" + collectionName;
        try (HttpSolrClient client = new HttpSolrClient(fromCollectionUrl)) {
          QueryResponse rsp = client.query(q);
          return rsp.getFacetFields().get(0).getValues();
        } catch (SolrServerException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public DocSet getDocSet() throws IOException {
      List<FacetField.Count> facets = facet(fromField);

      if (facets.isEmpty()) return DocSet.EMPTY;

      FixedBitSet resultBits = null;

      // minimum docFreq to use the cache
      int minDocFreqTo = Math.max(5, searcher.maxDoc() >> 13);

      // use a smaller size than normal since we will need to sort and dedup the results
      int maxSortedIntSize = Math.max(10, searcher.maxDoc() >> 10);

      List<DocSet> resultList = new ArrayList<>(10);

      Fields toFields = searcher.getLeafReader().fields();
      Terms toTerms = toFields.terms(toField);
      if (toTerms == null) return DocSet.EMPTY;

      FieldType fromFieldType = searcher.getSchema().getFieldType(fromField);
      String prefixStr = TrieField.getMainValuePrefix(fromFieldType);
      BytesRef prefix = prefixStr == null ? null : new BytesRef(prefixStr);

      TermsEnum  toTermsEnum = toTerms.iterator();
      SolrIndexSearcher.DocsEnumState toDeState = null;

      Bits toLiveDocs = searcher.getLeafReader().getLiveDocs();

      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.postingsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;

      for (FacetField.Count facet : facets) {
        BytesRef term = new BytesRef(fromFieldType.toInternal(facet.getName()));
        if (prefix != null && !StringHelper.startsWith(term, prefix)) {
          continue;
        }

        // TODO: instead of skipping here, pass the prefix into the facet query as facet.prefix?
        if (prefixStr != null && !facet.getName().startsWith(prefixStr)) {
          continue;
        }

        fromTermCount++;

        TermsEnum.SeekStatus status = toTermsEnum.seekCeil(term);
        if (status == TermsEnum.SeekStatus.END) break;
        if (status == TermsEnum.SeekStatus.FOUND) {
          fromTermHits++;
          fromTermHitsTotalDf += facet.getCount();
          toTermHits++;
          int df = toTermsEnum.docFreq();
          toTermHitsTotalDf += df;
          if (resultBits==null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
            resultBits = new FixedBitSet(searcher.maxDoc());
          }

          // if we don't have a bitset yet, or if the resulting set will be too large
          // use the filterCache to get a DocSet
          if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
            // use filter cache
            DocSet toTermSet = searcher.getDocSet(toDeState);
            resultListDocs += toTermSet.size();
            if (resultBits != null) {
              toTermSet.addAllTo(new BitDocSet(resultBits));
            } else {
              if (toTermSet instanceof BitDocSet) {
                resultBits = ((BitDocSet)toTermSet).bits.clone();
              } else {
                resultList.add(toTermSet);
              }
            }
          } else {
            toTermDirectCount++;

            // need to use liveDocs here so we don't map to any deleted ones
            toDeState.postingsEnum = toDeState.termsEnum.postings(toDeState.postingsEnum, PostingsEnum.NONE);
            toDeState.postingsEnum = BitsFilteredPostingsEnum.wrap(toDeState.postingsEnum, toDeState.liveDocs);
            PostingsEnum postingsEnum = toDeState.postingsEnum;

            if (postingsEnum instanceof MultiPostingsEnum) {
              MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
              int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
              for (int subindex = 0; subindex<numSubs; subindex++) {
                MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
                if (sub.postingsEnum == null) continue;
                int base = sub.slice.start;
                int docid;
                while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid + base);
                }
              }
            } else {
              int docid;
              while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                resultListDocs++;
                resultBits.set(docid);
              }
            }
          }
        }
      }

      smallSetsDeferred = resultList.size();

      if (resultBits != null) {
        BitDocSet bitSet = new BitDocSet(resultBits);
        for (DocSet set : resultList) {
          set.addAllTo(bitSet);
        }
        return bitSet;
      }

      if (resultList.size()==0) {
        return DocSet.EMPTY;
      }

      if (resultList.size() == 1) {
        return resultList.get(0);
      }

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;
      for (DocSet set : resultList) {
        System.arraycopy(((SortedIntDocSet)set).getDocs(), 0, docs, pos, set.size());
        pos += set.size();
      }
      Arrays.sort(docs);
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }

      return new SortedIntDocSet(dedup, dedup.length);
    }

  }

  @Override
  public String toString(String field) {
    return "{!fulljoin from=" + fromField + " to=" + toField
        + "}" + q.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    FullJoinQuery other = (FullJoinQuery)o;
    return this.fromField.equals(other.fromField)
           && this.toField.equals(other.toField)
           && this.q.equals(other.q)
        ;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = h * 31 + q.hashCode();
    h = h * 31 + fromField.hashCode();
    h = h * 31 + toField.hashCode();
    return h;
  }

}

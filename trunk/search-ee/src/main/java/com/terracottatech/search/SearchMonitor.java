package com.terracottatech.search;

import org.apache.lucene.search.Query;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author cschanck
 **/
public class SearchMonitor {
  private static long envIntervalSecs = Long.parseLong(System.getProperty("tc.searchmonitor.intervalMS", "0"));
  private static Query NULLQ = new Query() {
    @Override
    public String toString(String s) {
      return "unknown query";
    }
  };
  private final Logger log;
  private final TotalStatsHolder[] holders = new TotalStatsHolder[] { new TotalStatsHolder(), new TotalStatsHolder() };
  private volatile Timer timer;
  private volatile TimerTask task;
  private volatile long intervalMS;
  private volatile int currentIndex = 0;
  private volatile TotalStatsHolder currentHolder = holders[currentIndex];
  private volatile boolean dead = false;
  private ConcurrentHashMap<QueryID, QueryInfo> liveQueries = new ConcurrentHashMap<QueryID, QueryInfo>();

  public SearchMonitor(LoggerFactory logFactory) {
    this(logFactory, envIntervalSecs, TimeUnit.SECONDS);
  }

  public SearchMonitor(LoggerFactory logFactory, long interval, TimeUnit unit) {
    this.log = logFactory.getLogger(SearchMonitor.class);
    setInterval(interval, unit);
  }

  public synchronized void setInterval(long dur, TimeUnit unit) {
    if (task != null) {
      task.cancel();
      task = null;
    }
    if (timer != null) {
      timer.cancel();
      timer = null;
    }

    this.intervalMS = TimeUnit.MILLISECONDS.convert(dur, unit);

    if (intervalMS > 0) {
      this.timer = new Timer("Search monitor", true);
      this.task = new TimerTask() {
        @Override
        public void run() {
          logAndRotate();
        }
      };
      timer.schedule(task, intervalMS, intervalMS);
    } else {
      timer = null;
      task = null;
    }

  }

  public synchronized void shutdown() {
    if (!dead) {
      dead = true;
      setInterval(0, TimeUnit.SECONDS);
      liveQueries.clear();
    }
  }

  public void beginTrackingQuery(QueryID id,
                                 String indexName,
                                 int queryStackSize,
                                 Query query,
                                 final boolean includeKeys,
                                 final boolean includeValues,
                                 Set<String> attributeSet,
                                 final Set<String> groupByAttributes,
                                 final List<NVPair> sortAttributes,
                                 List<NVPair> aggPairs,
                                 int resultLimit,
                                 int batchLimit) {
    if (dead) {
      return;
    }
    QueryInfo qi = new QueryInfo(id,
                                 indexName,
                                 query,
                                 includeKeys,
                                 includeValues,
                                 attributeSet,
                                 groupByAttributes,
                                 sortAttributes,
                                 aggPairs,
                                 resultLimit,
                                 batchLimit);
    QueryInfo ret = liveQueries.putIfAbsent(id, qi);
    if (intervalMS > 0) {
      TotalStatsHolder p = currentHolder;

      p.searchCount.incrementAndGet();
      p.queryStackSizeCount.addAndGet(queryStackSize);
      p.aggregateCount.addAndGet(aggPairs.size());
      p.attributeSetCount.addAndGet(attributeSet.size());
      p.groupByAttributeCount.addAndGet(groupByAttributes.size());
      if (includeKeys) {
        p.includeKeysCount.incrementAndGet();
      }
      if (includeValues) {
        p.includeValuesCount.incrementAndGet();
      }
      p.sortAttributeCount.addAndGet(sortAttributes.size());

      // slushy. by design.
      int was = p.maxStackDepthCount.get();
      if (queryStackSize > was) {
        if (p.maxStackDepthSeen.compareAndSet(was, queryStackSize)) {
          p.maxStackDepthCount.set(0);
        }
      } else if (queryStackSize == was) {
        p.maxStackDepthCount.incrementAndGet();
      }
    }
  }

  public void trackQueryResults(QueryID id, int cnt) {
    if (dead || intervalMS <= 0) {
      return;
    }
    TotalStatsHolder p = currentHolder;
    p.resultCount.addAndGet(cnt);
  }

  public void finishTrackingQuery(QueryID id) {
    if (dead) {
      return;
    }
    liveQueries.remove(id);
  }

  public void logAndRotate() {
    loginfo();
    rotate();
  }

  private void loginfo() {
    log.info(toString());
  }

  public String getCumulativeStatsString() {
    TotalStatsHolder p = currentHolder;
    return p.toString();
  }

  public SortedSet<QueryInfo> getLiveQuerySnapshot() {
    TreeSet<QueryInfo> ts = new TreeSet<QueryInfo>(liveQueries.values());
    return ts;
  }

  @Override
  public String toString() {
    long now = System.nanoTime();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println(getCumulativeStatsString());
    for (QueryInfo qi : getLiveQuerySnapshot()) {
      pw.println("Live Search: " + qi.toString(now));
    }

    pw.flush();
    return sw.toString();
  }

  private synchronized void rotate() {
    TotalStatsHolder was = currentHolder;
    currentIndex = 1 - currentIndex;
    currentHolder = holders[currentIndex];
    was.reset();
  }

  static class QueryInfo implements Comparable<QueryInfo> {
    private final QueryID id;
    private final boolean keys;
    private final boolean values;
    private final Set<String> attributeSet;
    private final Set<String> groupByAttributes;
    private final List<NVPair> sortAttributes;
    private final List<NVPair> aggPairs;
    private final int resultlimit;
    private final int batchLimit;
    private final Query query;
    private final String indexName;
    private final long startTimeNS;

    public QueryInfo(QueryID id,
                     String indexName,
                     Query query,
                     boolean keys,
                     boolean values,
                     Set<String> attributeSet,
                     final Set<String> groupByAttributes,
                     final List<NVPair> sortAttributes,
                     List<NVPair> aggPairs,
                     int resultLimit,
                     int batchLimit) {

      // aggressively guard for null, so toString() always works.
      this.startTimeNS = System.nanoTime();
      this.id = id == null ? new QueryID(-1, -1) : id;
      this.indexName = indexName == null ? "unknown" : indexName;
      this.query = query == null ? NULLQ : query;
      this.keys = keys;
      this.values = values;
      this.attributeSet = attributeSet == null ? Collections.EMPTY_SET : attributeSet;
      this.groupByAttributes = groupByAttributes == null ? Collections.EMPTY_SET : groupByAttributes;
      this.sortAttributes = sortAttributes == null ? Collections.EMPTY_LIST : sortAttributes;
      this.aggPairs = aggPairs == null ? Collections.EMPTY_LIST : aggPairs;
      this.resultlimit = resultLimit;
      this.batchLimit = batchLimit;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      QueryInfo info = (QueryInfo) o;

      return id.equals(info.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return toString(System.nanoTime());
    }

    public String toString(long nowNS) {
      try {
        // super sure.
        return id + " Index=" + indexName + " Search: [" + query.toString() + "] keys=" + keys + " values=" + values + " " + "attributeSet=" + attributeSet + " groupByAttributes=" + groupByAttributes + " sortAttributes=" + sortAttributes + " aggPairs=" + aggPairs + " resultLimit=" + resultlimit + " batchLimit=" + batchLimit + " (" + (nowNS - startTimeNS) + "ns)";
      } catch (Throwable t) {
        return "unprintable QueryInfo";
      }
    }

    @Override
    public int compareTo(QueryInfo o) {
      return (int) (id.queryId - o.id.queryId);
    }
  }

  class TotalStatsHolder {
    final AtomicLong searchCount = new AtomicLong(0);
    final AtomicLong queryStackSizeCount = new AtomicLong(0);
    final AtomicLong includeKeysCount = new AtomicLong(0);
    final AtomicLong includeValuesCount = new AtomicLong(0);
    final AtomicLong attributeSetCount = new AtomicLong(0);
    final AtomicLong groupByAttributeCount = new AtomicLong(0);
    final AtomicLong sortAttributeCount = new AtomicLong(0);
    final AtomicLong aggregateCount = new AtomicLong(0);
    final AtomicLong resultCount = new AtomicLong(0);
    final AtomicInteger maxStackDepthSeen = new AtomicInteger(0);
    final AtomicInteger maxStackDepthCount = new AtomicInteger(0);

    private void reset() {
      searchCount.set(0);
      queryStackSizeCount.set(0);
      includeKeysCount.set(0);
      includeValuesCount.set(0);
      attributeSetCount.set(0);
      groupByAttributeCount.set(0);
      sortAttributeCount.set(0);
      aggregateCount.set(0);
      resultCount.set(0);
      maxStackDepthSeen.set(0);
    }

    @Override
    public String toString() {
      return "Cumulative Search Stats: searches=" + searchCount + " searchTerms=" + queryStackSizeCount + " " + "keysIncluded=" + includeKeysCount + " valuesIncluded=" + includeValuesCount + " attributes=" + attributeSetCount + " groupBys=" + groupByAttributeCount + " sortAttributes=" + sortAttributeCount + " " + "aggregates=" + aggregateCount + " resultCount=" + resultCount + " " + "maxStackSize=" + maxStackDepthSeen + "/" + maxStackDepthCount
        .get();
    }
  }
}

package org.neo4j.backup.consistency.report;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.backup.consistency.RecordType;

public class ConsistencySummaryStats
{
    private final ConcurrentMap<RecordType, AtomicInteger> inconsistentRecordCount =
            new ConcurrentHashMap<RecordType, AtomicInteger>();
    private final AtomicInteger totalInconsistencyCount = new AtomicInteger();

    public boolean isConsistent()
    {
        return totalInconsistencyCount.get() == 0;
    }

    public int getInconsistencyCountForRecordType( RecordType recordType )
    {
        AtomicInteger counter = inconsistentRecordCount.get( recordType );
        return counter == null ? 0 : counter.get();
    }

    public int getTotalInconsistencyCount()
    {
        return totalInconsistencyCount.get();
    }

    void add( RecordType recordType, int errors, int warnings )
    {
        if ( errors > 0 )
        {
            AtomicInteger counter = inconsistentRecordCount.get( recordType );
            if ( counter == null )
            {
                AtomicInteger suggestion = new AtomicInteger();
                counter = inconsistentRecordCount.putIfAbsent( recordType, suggestion );
                if ( counter == null )
                {
                    counter = suggestion;
                }
            }
            counter.incrementAndGet();
            totalInconsistencyCount.incrementAndGet();
        }
    }
}

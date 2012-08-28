package org.neo4j.backup.consistency.report;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class ConsistencySummaryStats
{
    private final ConcurrentMap<Class<? extends AbstractBaseRecord>, AtomicInteger> inconsistentRecordCount =
            new ConcurrentHashMap<Class<? extends AbstractBaseRecord>, AtomicInteger>();
    private final AtomicInteger totalInconsistencyCount = new AtomicInteger();

    public boolean isConsistent()
    {
        return totalInconsistencyCount.get() == 0;
    }

    public int getInconsistencyCountForRecordType( Class<? extends AbstractBaseRecord> recordType )
    {
        AtomicInteger counter = inconsistentRecordCount.get( recordType );
        return counter == null ? 0 : counter.get();
    }

    public int getTotalInconsistencyCount()
    {
        return totalInconsistencyCount.get();
    }

    void add( Class<? extends AbstractBaseRecord> recordType, int errors, int warnings )
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

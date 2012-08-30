package org.neo4j.backup.consistency.report;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.backup.consistency.RecordType;

public class ConsistencySummaryStatistics
{
    private final ConcurrentMap<RecordType, AtomicInteger> inconsistentRecordCount =
            new ConcurrentHashMap<RecordType, AtomicInteger>();
    private final AtomicInteger totalInconsistencyCount = new AtomicInteger();
    private final AtomicLong errorCount = new AtomicLong(), warningCount = new AtomicLong();

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() ).append( '{' );
        result.append( "\n\tNumber of errors: " ).append( errorCount );
        result.append( "\n\tNumber of warnings: " ).append( warningCount );
        for ( Map.Entry<RecordType, AtomicInteger> entry : inconsistentRecordCount.entrySet() )
        {
            result.append( "\n\tNumber of inconsistent " )
                  .append( entry.getKey() ).append( " records: " ).append( entry.getValue() );
        }
        return result.append( "\n}" ).toString();
    }

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
        errorCount.addAndGet( errors );
        warningCount.addAndGet( warnings );
    }
}

package org.neo4j.backup.consistency;

import org.neo4j.backup.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.kernel.impl.nioneo.store.DataInconsistencyError;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;

public class ConsistencyCheckingError extends DataInconsistencyError
{
    private final ConsistencySummaryStatistics summary;

    public ConsistencyCheckingError( LogEntry.Start startEntry, LogEntry.Commit commitEntry,
                                     ConsistencySummaryStatistics summary )
    {
        super( String.format( "Inconsistencies in transaction\n\t%s\n\t%s\n\t%s",
                              (startEntry == null ? "NO START ENTRY" : startEntry.toString()),
                              (commitEntry == null ? "NO COMMIT ENTRY" : commitEntry.toString()),
                              summary ) );
        this.summary = summary;
    }

    public int getInconsistencyCountForRecordType( RecordType recordType )
    {
        return summary.getInconsistencyCountForRecordType( recordType );
    }

    public int getTotalInconsistencyCount()
    {
        return summary.getTotalInconsistencyCount();
    }
}

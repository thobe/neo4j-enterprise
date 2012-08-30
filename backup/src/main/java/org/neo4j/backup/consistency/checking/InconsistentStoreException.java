package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencySummaryStatistics;

public class InconsistentStoreException extends Throwable
{
    private final ConsistencySummaryStatistics summary;

    public InconsistentStoreException( ConsistencySummaryStatistics summary )
    {
        super( summary.toString() );
        this.summary = summary;
    }

    public ConsistencySummaryStatistics summary()
    {
        return summary;
    }
}

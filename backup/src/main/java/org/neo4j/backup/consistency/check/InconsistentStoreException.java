package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.report.ConsistencySummaryStats;

public class InconsistentStoreException extends Throwable
{
    private final ConsistencySummaryStats summary;

    public InconsistentStoreException( ConsistencySummaryStats summary )
    {
        this.summary = summary;
    }

    public ConsistencySummaryStats summary()
    {
        return summary;
    }
}

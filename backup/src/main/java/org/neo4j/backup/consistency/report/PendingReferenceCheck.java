package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class PendingReferenceCheck<REFERENCED extends AbstractBaseRecord>
{
    private ConsistencyReport report;
    private final ComparativeRecordChecker checker;

    PendingReferenceCheck( ConsistencyReport report, ComparativeRecordChecker checker )
    {
        this.report = report;
        this.checker = checker;
    }

    public void checkReference( REFERENCED referenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchReference( report(), checker, referenced, records );
    }

    public void checkDiffReference( REFERENCED oldReferenced, REFERENCED newReferenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchChangeReference( report(), checker, oldReferenced, newReferenced, records );
    }

    public synchronized void skip()
    {
        if ( report != null )
        {
            ConsistencyReporter.dispatchSkip( report );
            report = null;
        }
    }

    private synchronized ConsistencyReport report()
    {
        if ( report == null )
        {
            throw new IllegalStateException( "Reference has already been checked." );
        }
        try
        {
            return report;
        }
        finally
        {
            report = null;
        }
    }
}

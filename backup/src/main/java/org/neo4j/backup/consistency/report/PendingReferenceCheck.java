package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
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

    public void checkReference( REFERENCED referenced )
    {
        ConsistencyReporter.dispatchReference( report(), checker, referenced );
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

    public void checkDiffReference( REFERENCED oldReferenced, REFERENCED newReferenced )
    {
        ConsistencyReporter.dispatchChangeReference( report(), checker, oldReferenced, newReferenced );
    }

    public synchronized void skip()
    {
        if ( report != null )
        {
            ConsistencyReporter.dispatchSkip( report );
            report = null;
        }
    }
}

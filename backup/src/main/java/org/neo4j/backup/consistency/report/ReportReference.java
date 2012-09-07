package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class ReportReference<REFERENCED extends AbstractBaseRecord>
{
    private final ConsistencyReport report;
    private final ComparativeRecordChecker checker;

    ReportReference( ConsistencyReport report, ComparativeRecordChecker checker )
    {
        this.report = report;
        this.checker = checker;
    }

    public void checkReference( REFERENCED referenced )
    {
        ConsistencyReporter.dispatchReference( report, checker, referenced );
    }

    public void checkDiffReference( REFERENCED oldReferenced, REFERENCED newReferenced )
    {
        ConsistencyReporter.dispatchChangeReference( report, checker, oldReferenced, newReferenced );
    }

    public void skip()
    {
        ConsistencyReporter.dispatchSkip( report );
    }
}

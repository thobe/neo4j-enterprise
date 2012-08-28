package org.neo4j.backup.consistency.check.incremental;

import org.neo4j.backup.consistency.check.InconsistentStoreException;
import org.neo4j.backup.consistency.check.full.FullCheck;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.report.ConsistencySummaryStats;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.impl.util.StringLogger;

public class IncrementalCheck
{
    private final StringLogger logger;

    public IncrementalCheck( StringLogger logger )
    {
        this.logger = logger;
    }

    public void check( DiffStore diffs ) throws InconsistentStoreException
    {
        ConsistencyReporter.SummarisingReporter reporter = ConsistencyReporter
                .create( new SimpleRecordAccess( diffs ), new DirectReferenceDispatcher(), logger );
        new FullCheck( false, Progress.Factory.NONE ).execute( diffs, reporter );
        ConsistencySummaryStats summary = reporter.getSummary();
        if ( !summary.isConsistent() )
        {
            throw new InconsistentStoreException( summary );
        }
    }
}

package org.neo4j.backup.consistency.checking.incremental;

import org.neo4j.backup.consistency.checking.InconsistentStoreException;
import org.neo4j.backup.consistency.checking.full.FullCheck;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullDiffCheck implements DiffCheck
{
    private final StringLogger logger;

    public FullDiffCheck( StringLogger logger )
    {
        this.logger = logger;
    }

    @Override
    public void check( DiffStore diffs ) throws InconsistentStoreException
    {
        ConsistencyReporter.SummarisingReporter reporter = ConsistencyReporter
                .create( new SimpleRecordAccess( diffs ), new DirectReferenceDispatcher(), logger );
        new FullCheck( false, Progress.Factory.NONE ).execute( diffs, reporter );
        ConsistencySummaryStatistics summary = reporter.getSummary();
        if ( !summary.isConsistent() )
        {
            throw new InconsistentStoreException( summary );
        }
    }
}

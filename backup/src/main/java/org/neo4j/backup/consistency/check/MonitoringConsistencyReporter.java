package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

public class MonitoringConsistencyReporter implements InconsistencyReport
{
    private final ConsistencyReporter reporter;
    private int inconsistencyCount = 0;

    public MonitoringConsistencyReporter( ConsistencyReporter reporter )
    {
        this.reporter = reporter;
    }

    // Inconsistency between two records
    public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
            InconsistencyType type )
    {
        reporter.report( recordStore, record, referredStore, referred, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    public <R extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R> store, R record, R referred, InconsistencyType type )
    {
        reporter.report( store, record, store, referred, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    // Internal inconsistency in a single record
    public <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, InconsistencyType type )
    {
        reporter.report( store, record, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    /**
     * Check if any inconsistencies was found by the checker. This method should
     * be invoked at the end of the check. If inconsistencies were found an
     * {@link AssertionError} summarizing the number of inconsistencies will be
     * thrown.
     *
     * @throws AssertionError if any inconsistencies were found.
     */
    public void checkResult() throws AssertionError
    {
        if ( inconsistencyCount != 0 )
        {
            throw new AssertionError(
                    String.format( "Store level inconsistency found in %d places", inconsistencyCount ) );
        }
    }
}

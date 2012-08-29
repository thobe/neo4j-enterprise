package org.neo4j.backup.consistency.check.incremental;

import org.neo4j.backup.consistency.check.InconsistentStoreException;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.kernel.impl.util.StringLogger;

public class IncrementalDiffCheck implements DiffCheck
{
    public IncrementalDiffCheck( StringLogger logger )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void check( DiffStore diffs ) throws InconsistentStoreException
    {
    }
}

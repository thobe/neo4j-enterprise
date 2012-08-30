package org.neo4j.backup.consistency.checking.incremental;

import org.neo4j.backup.consistency.checking.InconsistentStoreException;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.kernel.impl.util.StringLogger;

public class IncrementalDiffCheck implements DiffCheck
{
    private final StringLogger logger;

    public IncrementalDiffCheck( StringLogger logger )
    {
        this.logger = logger;
    }

    @Override
    public void check( DiffStore diffs ) throws InconsistentStoreException
    {
        // TODO: implement this...
    }
}

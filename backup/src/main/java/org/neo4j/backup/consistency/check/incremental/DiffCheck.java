package org.neo4j.backup.consistency.check.incremental;

import org.neo4j.backup.consistency.check.InconsistentStoreException;
import org.neo4j.backup.consistency.store.DiffStore;

public interface DiffCheck
{
    void check( DiffStore diffs ) throws InconsistentStoreException;
}

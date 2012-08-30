package org.neo4j.backup.consistency.checking.incremental;

import org.neo4j.backup.consistency.checking.InconsistentStoreException;
import org.neo4j.backup.consistency.store.DiffStore;

public interface DiffCheck
{
    void check( DiffStore diffs ) throws InconsistentStoreException;
}

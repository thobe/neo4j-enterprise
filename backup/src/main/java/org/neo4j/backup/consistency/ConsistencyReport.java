package org.neo4j.backup.consistency;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public interface ConsistencyReport
{
    void nodeRelationshipNotInUse( NodeRecord inconsistent, RelationshipRecord referenced );
}

package org.neo4j.backup.consistency.repair;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_INVALID;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_NOT_IN_USE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_NODE_INVALID;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_NODE_NOT_IN_USE;

public enum RelationshipNodeField
{
    FIRST( SOURCE_NODE_INVALID, SOURCE_NODE_NOT_IN_USE )
    {
        @Override
        public long get( RelationshipRecord rel )
        {
            return rel.getFirstNode();
        }
    },
    SECOND( TARGET_NODE_INVALID, TARGET_NODE_NOT_IN_USE )
    {
        @Override
        public long get( RelationshipRecord rel )
        {
            return rel.getSecondNode();
        }
    };
    public final InconsistencyType.ReferenceInconsistency invalidReference, notInUse;

    public abstract long get( RelationshipRecord rel );

    RelationshipNodeField( InconsistencyType.ReferenceInconsistency invalidReference,
                           InconsistencyType.ReferenceInconsistency notInUse )
    {
        this.invalidReference = invalidReference;
        this.notInUse = notInUse;
    }
}

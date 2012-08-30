package org.neo4j.backup.consistency.repair;

import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public enum RelationshipChainDirection
{
    NEXT( RelationshipChainField.FIRST_NEXT, RelationshipChainField.SECOND_NEXT ),
    PREV( RelationshipChainField.FIRST_PREV, RelationshipChainField.SECOND_PREV );

    private final RelationshipChainField first;
    private final RelationshipChainField second;

    RelationshipChainDirection( RelationshipChainField first, RelationshipChainField second )
    {
        this.first = first;
        this.second = second;
    }

    public RelationshipChainField fieldFor( long nodeId, RelationshipRecord rel )
    {
        if (rel.getFirstNode() == nodeId)
        {
            return first;
        }
        else if (rel.getSecondNode() == nodeId)
        {
            return second;
        }
        throw new IllegalArgumentException( String.format( "%d does not reference node %d", rel, nodeId ) );
    }
}

package org.neo4j.backup.consistency;

import static org.neo4j.backup.consistency.RelationshipChainField.FIRST_NEXT;
import static org.neo4j.backup.consistency.RelationshipChainField.FIRST_PREV;
import static org.neo4j.backup.consistency.RelationshipChainField.SECOND_NEXT;
import static org.neo4j.backup.consistency.RelationshipChainField.SECOND_PREV;

import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public enum RelationshipChainDirection
{
    NEXT( FIRST_NEXT, SECOND_NEXT ),
    PREV( FIRST_PREV, SECOND_PREV );

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

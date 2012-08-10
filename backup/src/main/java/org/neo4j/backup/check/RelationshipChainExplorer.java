package org.neo4j.backup.check;

import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.FIRST_NEXT;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.FIRST_PREV;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.SECOND_NEXT;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.SECOND_PREV;
import static org.neo4j.backup.check.RelationshipChainExplorer.RelationshipChainDirection.NEXT;
import static org.neo4j.backup.check.RelationshipChainExplorer.RelationshipChainDirection.PREV;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class RelationshipChainExplorer
{
    public static final int none = Record.NO_NEXT_RELATIONSHIP.intValue();
    private final RecordStore<RelationshipRecord> recordStore;

    public RelationshipChainExplorer( RecordStore<RelationshipRecord> recordStore )
    {
        this.recordStore = recordStore;
    }

    public RecordSet<RelationshipRecord> exploreRelationshipRecordChainsToDepthTwo( RelationshipRecord record )
    {
        RecordSet<RelationshipRecord> records = new RecordSet<RelationshipRecord>();
        for ( ConsistencyCheck.NodeField nodeField : ConsistencyCheck.NodeField.values() )
        {
            long nodeId = nodeField.get( record );
            records.addAll( expandChains( expandChainInBothDirections( record, nodeId ), nodeId ) );
        }
        return records;
    }

    private RecordSet<RelationshipRecord> expandChains( RecordSet<RelationshipRecord> records, long otherNodeId )
    {
        RecordSet<RelationshipRecord> chains = new RecordSet<RelationshipRecord>();
        for ( RelationshipRecord record : records )
        {
            chains.addAll( expandChainInBothDirections( record,
                    record.getFirstNode() == otherNodeId ? record.getSecondNode() : record.getFirstNode() ) );
        }
        return chains;
    }

    private RecordSet<RelationshipRecord> expandChainInBothDirections( RelationshipRecord record, long nodeId )
    {
        return expandChain( record, nodeId, PREV ).union( expandChain( record, nodeId, NEXT ) );
    }

    protected RecordSet<RelationshipRecord> followChainFromNode(long nodeId, long relationshipId )
    {
        RelationshipRecord record = recordStore.getRecord( relationshipId );
        return expandChain( record, nodeId, NEXT );
    }

    private RecordSet<RelationshipRecord> expandChain( RelationshipRecord record, long nodeId,
                                                       RelationshipChainDirection direction )
    {
        RecordSet<RelationshipRecord> chain = new RecordSet<RelationshipRecord>();
        chain.add( record );
        RelationshipRecord currentRecord = record;
        long nextRelId;
        while ( currentRecord.inUse() &&
                (nextRelId = direction.fieldFor( nodeId, currentRecord ).relOf( currentRecord )) != none ) {
            currentRecord = recordStore.forceGetRecord( nextRelId );
            chain.add( currentRecord );
        }
        return chain;
    }

    protected enum RelationshipChainDirection
    {
        NEXT( FIRST_NEXT, SECOND_NEXT ),
        PREV( FIRST_PREV, SECOND_PREV );

        private final ConsistencyCheck.RelationshipField first;
        private final ConsistencyCheck.RelationshipField second;

        private RelationshipChainDirection( ConsistencyCheck.RelationshipField first, ConsistencyCheck.RelationshipField second )
        {
            this.first = first;
            this.second = second;
        }

        protected ConsistencyCheck.RelationshipField fieldFor( long nodeId, RelationshipRecord rel )
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
}

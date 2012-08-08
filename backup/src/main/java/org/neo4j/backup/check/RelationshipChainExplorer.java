package org.neo4j.backup.check;

import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.FIRST_NEXT;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.FIRST_PREV;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.SECOND_NEXT;
import static org.neo4j.backup.check.ConsistencyCheck.RelationshipField.SECOND_PREV;

import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class RelationshipChainExplorer
{
    private final RecordStore<RelationshipRecord> recordStore;

    public RelationshipChainExplorer( RecordStore<RelationshipRecord> recordStore )
    {
        this.recordStore = recordStore;
    }

    public RecordSet<RelationshipRecord> exploreRelationshipRecordChainsToDepthTwo( RelationshipRecord record )
    {
        RecordSet<RelationshipRecord> records = new RecordSet<RelationshipRecord>();
        records.addAll( expandChains(
                expandChainInBothDirections( record, FIRST_PREV, FIRST_NEXT ), SECOND_PREV, SECOND_NEXT ) );
        records.addAll( expandChains(
                expandChainInBothDirections( record, SECOND_PREV, SECOND_NEXT ), FIRST_PREV, FIRST_NEXT ) );
        return records;
    }

    private RecordSet<RelationshipRecord> expandChains( RecordSet<RelationshipRecord> records,
                                                                   ConsistencyCheck.RelationshipField prevField,
                                                                   ConsistencyCheck.RelationshipField nextField )
    {
        RecordSet<RelationshipRecord> chains = new RecordSet<RelationshipRecord>();
        for ( RelationshipRecord record : records )
        {
            chains.addAll( expandChainInBothDirections( record, prevField, nextField ) );
        }
        return chains;
    }

    private RecordSet<RelationshipRecord> expandChainInBothDirections( RelationshipRecord record,
                                                                       ConsistencyCheck.RelationshipField prevField,
                                                                       ConsistencyCheck.RelationshipField nextField )
    {
        return expandChain( record, prevField ).union( expandChain( record, nextField ) );
    }

    private RecordSet<RelationshipRecord> expandChain( RelationshipRecord record,
                                                        ConsistencyCheck.RelationshipField field )
    {
        RecordSet<RelationshipRecord> chain = new RecordSet<RelationshipRecord>();
        chain.add( record );
        RelationshipRecord currentRecord = record;
        while ( currentRecord.inUse() && field.relOf( currentRecord ) != field.none) {
            currentRecord = recordStore.forceGetRecord( field.relOf( currentRecord ) );
            chain.add( currentRecord );
        }
        return chain;
    }

}

package org.neo4j.backup.check;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class OwningNodeRelationshipChain
{
    private final RelationshipChainExplorer relationshipChainExplorer;
    private final RecordStore<NodeRecord> nodeStore;

    public OwningNodeRelationshipChain( RelationshipChainExplorer relationshipChainExplorer,
                                        RecordStore<NodeRecord> nodeStore )
    {
        this.relationshipChainExplorer = relationshipChainExplorer;
        this.nodeStore = nodeStore;
    }

    public RecordSet<RelationshipRecord> findRelationshipChainsThatThisRecordShouldBelongTo(
            RelationshipRecord relationship )
    {
        RecordSet<RelationshipRecord> records = new RecordSet<RelationshipRecord>();
        for ( ConsistencyCheck.NodeField field : ConsistencyCheck.NodeField.values() )
        {
            long nodeId = field.get( relationship );
            NodeRecord nodeRecord = nodeStore.forceGetRecord( nodeId );
            records.addAll( relationshipChainExplorer.followChainFromNode( nodeId, nodeRecord.getNextRel() ) );
        }
        return records;
    }


}

package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class NodeRecordCheck extends PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    NodeRecordCheck()
    {
        super( NodeField.RELATIONSHIP );
    }

    @Override
    PropertyOwner owner( NodeRecord record )
    {
        return new PropertyOwner.OwningNode( record.getId() );
    }

    private enum NodeField implements RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>,
            ComparativeRecordChecker<NodeRecord, RelationshipRecord, ConsistencyReport.NodeConsistencyReport>
    {
        RELATIONSHIP
        {
            @Override
            public void checkConsistency( NodeRecord node, ConsistencyReport.NodeConsistencyReport report,
                                          RecordReferencer records )
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.is( node.getNextRel() ) )
                {
                    report.forReference( records.relationship( node.getNextRel() ), this );
                }
            }

            @Override
            public void checkReference( NodeRecord node, RelationshipRecord relationship,
                                        ConsistencyReport.NodeConsistencyReport report )
            {
                if ( !relationship.inUse() )
                {
                    report.relationshipNotInUse( relationship );
                }
                else
                {
                    RelationshipNodeField selectedField = RelationshipNodeField.select( relationship, node );
                    if ( selectedField == null )
                    {
                        report.relationshipForOtherNode( relationship );
                    }
                    else
                    {
                        RelationshipNodeField[] fields;
                        if ( relationship.getFirstNode() == relationship.getSecondNode() )
                        { // this relationship is a loop, report both inconsistencies
                            fields = RelationshipNodeField.values();
                        }
                        else
                        {
                            fields = new RelationshipNodeField[]{selectedField};
                        }
                        for ( RelationshipNodeField field : fields )
                        {
                            if ( !Record.NO_NEXT_RELATIONSHIP.is( field.prev( relationship ) ) )
                            {
                                field.notFirstInChain( report, relationship );
                            }
                        }
                    }
                }
            }
        }
    }
}

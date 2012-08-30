package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class NodeRecordCheck extends PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    public NodeRecordCheck()
    {
        super( NodeField.RELATIONSHIP );
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

            @Override
            public long valueFrom( NodeRecord record )
            {
                return record.getNextRel();
            }

            @Override
            public boolean isNone( NodeRecord record )
            {
                return Record.NO_NEXT_RELATIONSHIP.is( record.getNextRel() );
            }

            @Override
            public boolean referencedRecordChanged( DiffRecordReferencer records, NodeRecord record )
            {
                return records.changedRelationship( record.getNextRel() ) != null;
            }

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.NodeConsistencyReport report )
            {
                report.relationshipReplacedButNotUpdated();
            }
        };
    }
}

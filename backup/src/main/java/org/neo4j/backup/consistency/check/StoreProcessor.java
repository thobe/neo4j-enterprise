package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static java.lang.String.format;

public class StoreProcessor extends RecordStore.Processor
{
    private ConsistencyReport.Reporter report;
    private RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker;
    private RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker;
    private RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyChecker;
    private RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> propertyKeyChecker;
    private RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> relationshipLabelChecker;

    public StoreProcessor( boolean checkPropertyOwners, ConsistencyReport.Reporter report )
    {
        this.report = report;
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( checkPropertyOwners );
        this.nodeChecker = decorator.decorate( new NodeRecordCheck() );
        this.relationshipChecker = decorator.decorate( new RelationshipRecordCheck() );
        this.propertyChecker = new PropertyRecordCheck();
        this.propertyKeyChecker = new PropertyKeyRecordCheck();
        this.relationshipLabelChecker = new RelationshipLabelRecordCheck();
    }

    @Override
    public void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        report.forNode( node, nodeChecker );
    }

    @Override
    public void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
    {
        report.forRelationship( rel, relationshipChecker );
    }

    @Override
    public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        report.forProperty( property, propertyChecker );
    }

    @Override
    public void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
    {
        RecordType type;
        switch ( idType )
        {
        case STRING_BLOCK:
            type = RecordType.STRING_PROPERTY;
            break;
        case RELATIONSHIP_TYPE_BLOCK:
            type = RecordType.RELATIONSHIP_LABEL_NAME;
            break;
        case PROPERTY_INDEX_BLOCK:
            type = RecordType.PROPERTY_KEY_NAME;
            break;
        default:
            throw new IllegalArgumentException( format( "The id type [%s] is not valid for String records.", idType ) );
        }
        report
        .forDynamicBlock( type, string, new DynamicRecordCheck( store, DynamicRecordCheck.StoreDereference.STRING ) );
    }

    @Override
    public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        report.forDynamicBlock( RecordType.ARRAY_PROPERTY, array,
                                new DynamicRecordCheck( store, DynamicRecordCheck.StoreDereference.ARRAY ) );
    }

    @Override
    public void processRelationshipType( RecordStore<RelationshipTypeRecord> store, RelationshipTypeRecord record )
    {
        report.forRelationshipLabel( record, relationshipLabelChecker );
    }

    @Override
    public void processPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord record )
    {
        report.forPropertyKey( record, propertyKeyChecker );
    }
}

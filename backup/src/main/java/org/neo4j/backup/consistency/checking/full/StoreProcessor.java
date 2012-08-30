package org.neo4j.backup.consistency.checking.full;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.DynamicRecordCheck;
import org.neo4j.backup.consistency.checking.NodeRecordCheck;
import org.neo4j.backup.consistency.checking.PropertyKeyRecordCheck;
import org.neo4j.backup.consistency.checking.PropertyRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.checking.RelationshipLabelRecordCheck;
import org.neo4j.backup.consistency.checking.RelationshipRecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static java.lang.String.format;
import static org.neo4j.backup.consistency.checking.DynamicRecordCheck.StoreDereference.ARRAY;

class StoreProcessor extends RecordStore.Processor
{
    private final ConsistencyReport.Reporter report;
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker;
    private final RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker;
    private final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyChecker;
    private final RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> propertyKeyChecker;
    private final RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> relationshipLabelChecker;
    private final PropertyOwnerCheck ownerCheck;

    public StoreProcessor( boolean checkPropertyOwners, ConsistencyReport.Reporter report )
    {
        this.report = report;
        this.ownerCheck = new PropertyOwnerCheck( checkPropertyOwners );
        this.nodeChecker = ownerCheck.decoratePrimitiveChecker( new NodeRecordCheck() );
        this.relationshipChecker = ownerCheck.decoratePrimitiveChecker( new RelationshipRecordCheck() );
        this.propertyChecker = ownerCheck.decoratePropertyChecker( new PropertyRecordCheck() );
        this.propertyKeyChecker = new PropertyKeyRecordCheck();
        this.relationshipLabelChecker = new RelationshipLabelRecordCheck();
    }

    void checkOrphanPropertyChains( Progress.Factory progressFactory )
    {
        ownerCheck.scanForOrphanChains( report, progressFactory );
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
        DynamicRecordCheck.StoreDereference dereference;
        switch ( idType )
        {
        case STRING_BLOCK:
            type = RecordType.STRING_PROPERTY;
            dereference = DynamicRecordCheck.StoreDereference.STRING;
            break;
        case RELATIONSHIP_TYPE_BLOCK:
            type = RecordType.RELATIONSHIP_LABEL_NAME;
            dereference = DynamicRecordCheck.StoreDereference.RELATIONSHIP_LABEL;
            break;
        case PROPERTY_INDEX_BLOCK:
            type = RecordType.PROPERTY_KEY_NAME;
            dereference = DynamicRecordCheck.StoreDereference.PROPERTY_KEY;
            break;
        default:
            throw new IllegalArgumentException( format( "The id type [%s] is not valid for String records.", idType ) );
        }
        report.forDynamicBlock( type, string, new DynamicRecordCheck( store, dereference ) );
    }

    @Override
    public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        report.forDynamicBlock( RecordType.ARRAY_PROPERTY, array, new DynamicRecordCheck( store, ARRAY ) );
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

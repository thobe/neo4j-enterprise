package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

@SuppressWarnings("unchecked")
public class SkippingRecordAccess implements DiffRecordAccess
{
    private static final RecordReference SKIP = new RecordReference()
    {
        @Override
        public void dispatch( PendingReferenceCheck reporter )
        {
            reporter.skip();
        }
    };

    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<RelationshipTypeRecord> relationshipLabel( int id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<PropertyIndexRecord> propertyKey( int id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<DynamicRecord> relationshipLabelName( int id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return SKIP;
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return SKIP;
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return SKIP;
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return null;
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return null;
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return null;
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return null;
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return null;
    }
}

package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.check.ComparativeRecordChecker;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RecordReferencer
{
    private final RecordAccess access;

    public RecordReferencer( RecordAccess access )
    {
        this.access = access;
    }

    public RecordReference<NodeRecord> node( final long id )
    {
        return new RecordReference<NodeRecord>()
        {
            @Override
            public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>> void dispatch(
                    ComparativeRecordChecker<RECORD, NodeRecord, REPORT> checker, RECORD record, REPORT report )
            {
                checker.checkReference( record, access.getNode( id ), report );
            }
        };
    }

    public RecordReference<RelationshipRecord> relationship( final long id )
    {
        return new RecordReference<RelationshipRecord>()
        {
            @Override
            public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>> void dispatch(
                    ComparativeRecordChecker<RECORD, RelationshipRecord, REPORT> checker, RECORD record, REPORT report )
            {
                checker.checkReference( record, access.getRelationship( id ), report );
            }
        };
    }

    public RecordReference<PropertyRecord> property( final long id )
    {
        return new RecordReference<PropertyRecord>()
        {
            @Override
            public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>> void dispatch(
                    ComparativeRecordChecker<RECORD, PropertyRecord, REPORT> checker, RECORD record, REPORT report )
            {
                checker.checkReference( record, access.getProperty( id ), report );
            }
        };
    }

    public RecordReference<RelationshipTypeRecord> relationshipLabel( final int id )
    {
        return new RecordReference<RelationshipTypeRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, RelationshipTypeRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getType( id ), report );
            }
        };
    }

    public RecordReference<PropertyIndexRecord> propertyKey( final int id )
    {
        return new RecordReference<PropertyIndexRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, PropertyIndexRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getKey( id ), report );
            }
        };
    }

    public RecordReference<DynamicRecord> string( final long id )
    {
        return new RecordReference<DynamicRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, DynamicRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getString( id ), report );
            }
        };
    }

    public RecordReference<DynamicRecord> array( final long id )
    {
        return new RecordReference<DynamicRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, DynamicRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getArray( id ), report );
            }
        };
    }

    public RecordReference<DynamicRecord> relationshipLabelName( final int id )
    {
        return new RecordReference<DynamicRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, DynamicRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getLabelName( id ), report );
            }
        };
    }

    public RecordReference<DynamicRecord> propertyKeyName( final int id )
    {
        return new RecordReference<DynamicRecord>()
        {
            @Override
            public <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
                    ComparativeRecordChecker<R, DynamicRecord, REPORT> checker, R record, REPORT report )
            {
                checker.checkReference( record, access.getKeyName( id ), report );
            }
        };
    }
}

package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

// TODO: it would be great if this also checked for cyclic chains. (we would also need cycle checking for full check, and for relationships)
enum OwnerChain
        implements ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
{
    OLD
    {
        @Override
        RecordReference<PropertyRecord> property( DiffRecordAccess records, long id )
        {
            return records.previousProperty( id );
        }

        @Override
        RecordReference<NodeRecord> node( DiffRecordAccess records, long id )
        {
            return records.previousNode( id );
        }

        @Override
        RecordReference<RelationshipRecord> relationship( DiffRecordAccess records, long id )
        {
            return records.previousRelationship( id );
        }

        @Override
        RecordReference<NeoStoreRecord> graph( DiffRecordAccess records )
        {
            return records.previousGraph();
        }

        @Override
        void wrongOwner( ConsistencyReport.PropertyConsistencyReport report )
        {
            report.changedForWrongOwner();
        }
    },

    NEW
    {
        @Override
        RecordReference<PropertyRecord> property( DiffRecordAccess records, long id )
        {
            return records.property( id );
        }

        @Override
        RecordReference<NodeRecord> node( DiffRecordAccess records, long id )
        {
            return records.node( id );
        }

        @Override
        RecordReference<RelationshipRecord> relationship( DiffRecordAccess records, long id )
        {
            return records.relationship( id );
        }

        @Override
        RecordReference<NeoStoreRecord> graph( DiffRecordAccess records )
        {
            return records.graph();
        }

        @Override
        void wrongOwner( ConsistencyReport.PropertyConsistencyReport report )
        {
            report.ownerDoesNotReferenceBack();
        }
    };

    private final ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>
            OWNER_CHECK =
            new ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void checkReference( PropertyRecord record, PrimitiveRecord owner,
                                            ConsistencyReport.PropertyConsistencyReport report, RecordAccess records )
                {
                    if ( !owner.inUse() || Record.NO_NEXT_PROPERTY.is( owner.getNextProp() ) )
                    {
                        wrongOwner( report );
                    }
                    else if ( owner.getNextProp() != record.getId() )
                    {
                        report.forReference( property( (DiffRecordAccess) records, owner.getNextProp() ),
                                             OwnerChain.this );
                    }
                }
            };

    @Override
    public void checkReference( PropertyRecord record, PropertyRecord property,
                                ConsistencyReport.PropertyConsistencyReport report, RecordAccess records )
    {
        if ( record.getId() != property.getId() )
        {
            if ( !property.inUse() || Record.NO_NEXT_PROPERTY.is( property.getNextProp() ) )
            {
                wrongOwner( report );
            }
            else if ( property.getNextProp() != record.getId() )
            {
                report.forReference( property( (DiffRecordAccess) records, property.getNextProp() ), this );
            }
        }
    }

    void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                DiffRecordAccess records )
    {
        report.forReference( ownerOf( record, records ), OWNER_CHECK );
    }

    private RecordReference<? extends PrimitiveRecord> ownerOf( PropertyRecord record, DiffRecordAccess records )
    {
        if ( record.getNodeId() != -1 )
        {
            return node( records, record.getNodeId() );
        }
        else if ( record.getRelId() != -1 )
        {
            return relationship( records, record.getRelId() );
        }
        else
        {
            return graph( records );
        }
    }

    abstract RecordReference<PropertyRecord> property( DiffRecordAccess records, long id );

    abstract RecordReference<NodeRecord> node( DiffRecordAccess records, long id );

    abstract RecordReference<RelationshipRecord> relationship( DiffRecordAccess records, long id );

    abstract RecordReference<NeoStoreRecord> graph( DiffRecordAccess records );

    abstract void wrongOwner( ConsistencyReport.PropertyConsistencyReport report );
}

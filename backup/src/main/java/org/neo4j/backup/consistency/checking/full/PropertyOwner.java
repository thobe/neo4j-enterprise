package org.neo4j.backup.consistency.checking.full;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;

abstract class PropertyOwner
{
    final long id;

    PropertyOwner( long id )
    {
        this.id = id;
    }

    abstract RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels );

    abstract long otherOwnerOf( PropertyRecord prop );

    abstract long ownerOf( PropertyRecord prop );

    abstract InconsistencyType propertyNotRemoved();

    public abstract RecordReference<PrimitiveRecord> record( RecordReferencer records );

    abstract void checkOphanage( ConsistencyReport.Reporter report,
                                 RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> check );

    public static final class OwningNode extends PropertyOwner
    {
        OwningNode( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels )
        {
            return nodes;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public RecordReference<PrimitiveRecord> record( RecordReferencer records )
        {
            return (RecordReference)records.node( id );
        }

        @Override
        void checkOphanage( ConsistencyReport.Reporter report,
                            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> check )
        {
            // not an orphan - do nothing
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }
    }

    public static final class OwningRelationship extends PropertyOwner
    {
        OwningRelationship( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels )
        {
            return rels;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public RecordReference<PrimitiveRecord> record( RecordReferencer records )
        {
            return (RecordReference)records.relationship( id );
        }

        @Override
        void checkOphanage( ConsistencyReport.Reporter report,
                            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> check )
        {
            // not an orphan - do nothing
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }
    }
}

package org.neo4j.backup.consistency.check;

import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency
        .PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency
        .PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

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

    static final class OwningNode extends PropertyOwner
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

    static final class OwningRelationship extends PropertyOwner
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

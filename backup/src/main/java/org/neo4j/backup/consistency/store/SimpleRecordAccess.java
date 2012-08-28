package org.neo4j.backup.consistency.store;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

public class SimpleRecordAccess implements RecordAccess
{
    private final StoreAccess access;

    public SimpleRecordAccess( StoreAccess access )
    {
        this.access = access;
    }

    @Override
    public RelationshipRecord getRelationship( long id )
    {
        return access.getRelationshipStore().forceGetRecord( id );
    }

    @Override
    public PropertyRecord getProperty( long id )
    {
        return access.getPropertyStore().forceGetRecord( id );
    }

    @Override
    public NodeRecord getNode( long id )
    {
        return access.getNodeStore().forceGetRecord( id );
    }

    @Override
    public RelationshipTypeRecord getType( int id )
    {
        return access.getRelationshipTypeStore().forceGetRecord( id );
    }

    @Override
    public PropertyIndexRecord getKey( int id )
    {
        return access.getPropertyIndexStore().forceGetRecord( id );
    }

    @Override
    public DynamicRecord getString( long id )
    {
        return access.getStringStore().forceGetRecord( id );
    }

    @Override
    public DynamicRecord getArray( long id )
    {
        return access.getArrayStore().forceGetRecord( id );
    }

    @Override
    public DynamicRecord getLabelName( int id )
    {
        return access.getTypeNameStore().forceGetRecord( id );
    }

    @Override
    public DynamicRecord getKeyName( int id )
    {
        return access.getPropertyKeyStore().forceGetRecord( id );
    }
}

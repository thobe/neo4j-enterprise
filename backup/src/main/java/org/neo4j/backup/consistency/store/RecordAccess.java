package org.neo4j.backup.consistency.store;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public interface RecordAccess
{
    RelationshipRecord getRelationship( long id );

    PropertyRecord getProperty( long id );

    NodeRecord getNode( long id );

    RelationshipTypeRecord getType( int id );

    PropertyIndexRecord getKey( int id );

    DynamicRecord getString( long id );

    DynamicRecord getArray( long id );

    DynamicRecord getLabelName( int id );

    DynamicRecord getKeyName( int id );
}

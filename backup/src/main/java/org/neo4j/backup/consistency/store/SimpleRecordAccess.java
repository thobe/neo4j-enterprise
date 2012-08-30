/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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

    @Override
    public NodeRecord changedNode( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getNodeStore().getChangedRecord( id ) : null;
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getRelationshipStore().getChangedRecord( id ) : null;
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getPropertyStore().getChangedRecord( id ) : null;
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getStringStore().getChangedRecord( id ) : null;
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getArrayStore().getChangedRecord( id ) : null;
    }
}

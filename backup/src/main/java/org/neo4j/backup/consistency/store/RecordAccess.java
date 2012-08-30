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

    NodeRecord changedNode( long id );

    RelationshipRecord changedRelationship( long id );

    PropertyRecord changedProperty( long id );

    DynamicRecord changedString( long id );

    DynamicRecord changedArray( long id );
}

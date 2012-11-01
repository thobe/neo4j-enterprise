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
package org.neo4j.consistency.repair.tool;

import org.neo4j.consistency.repair.RecordSet;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class ConsistencyCheckOutput
{
    private final RecordSet<NodeRecord> nodeRecords = new RecordSet<NodeRecord>();
    private final RecordSet<RelationshipRecord> relationshipRecords = new RecordSet<RelationshipRecord>();
    private final RecordSet<PropertyRecord> propertyRecords = new RecordSet<PropertyRecord>();

    public RecordSet<NodeRecord> getNodeRecords()
    {
        return nodeRecords;
    }

    public RecordSet<RelationshipRecord> getRelationshipRecords()
    {
        return relationshipRecords;
    }

    public RecordSet<PropertyRecord> getPropertyRecords()
    {
        return propertyRecords;
    }
}

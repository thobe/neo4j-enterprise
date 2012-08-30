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

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RecordReferencer
{
    protected final RecordAccess access;

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

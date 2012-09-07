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
package org.neo4j.backup.consistency.checking.full;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.checking.PrimitiveRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

class PropertyOwnerCheck
{
    private final ConcurrentMap<Long, PropertyOwner> owners;

    PropertyOwnerCheck( boolean active )
    {
        this.owners = active ? new ConcurrentHashMap<Long, PropertyOwner>( 16, 0.75f, 4 ) : null;
    }

    void scanForOrphanChains( ProgressMonitorFactory progressFactory )
    {
        if ( owners != null )
        {
            ProgressListener progress = progressFactory
                    .singlePart( "Checking for orphan property chains", owners.size() );
            for ( PropertyOwner owner : owners.values() )
            {
                owner.checkOrphanage();
                progress.add( 1 );
            }
            progress.done();
        }
    }

    RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<NodeRecord, ConsistencyReport.NodeConsistencyReport>( checker )
        {
            PropertyOwner owner( NodeRecord record )
            {
                return new PropertyOwner.OwningNode( record.getId() );
            }
        };
    }

    RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>(
                checker )
        {
            PropertyOwner owner( RelationshipRecord record )
            {
                return new PropertyOwner.OwningRelationship( record.getId() );
            }
        };
    }

    RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                               RecordAccess records )
            {
                if ( record.inUse() && Record.NO_PREVIOUS_PROPERTY.is( record.getPrevProp() ) )
                { // this record is first in a chain
                    PropertyOwner.UnknownOwner owner = new PropertyOwner.UnknownOwner();
                    report.forReference( owner, ORPHAN_CHECKER );
                    if ( null == owners.putIfAbsent( record.getId(), owner ) )
                    {
                        owner.skip();
                    }
                }
                checker.check( record, report, records );
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     ConsistencyReport.PropertyConsistencyReport report, DiffRecordAccess records )
            {
                checker.checkChange( oldRecord, newRecord, report, records );
            }
        };
    }

    private abstract class PrimitiveCheckerDecorator<RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport<RECORD, REPORT>>
            implements RecordCheck<RECORD, REPORT>
    {
        private final PrimitiveRecordCheck<RECORD, REPORT> checker;

        PrimitiveCheckerDecorator( PrimitiveRecordCheck<RECORD, REPORT> checker )
        {
            this.checker = checker;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void check( RECORD record, REPORT report, RecordAccess records )
        {
            if ( record.inUse() )
            {
                long prop = record.getNextProp();
                if ( !Record.NO_NEXT_PROPERTY.is( prop ) )
                {
                    PropertyOwner previous = owners.put( prop, owner( record ) );
                    if ( previous != null )
                    {
                        report.forReference( previous.record( records ), checker.ownerCheck );
                    }
                }
            }
            checker.check( record, report, records );
        }

        @Override
        public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordAccess records )
        {
            checker.checkChange( oldRecord, newRecord, report, records );
        }

        abstract PropertyOwner owner( RECORD record );
    }

    private static final ComparativeRecordChecker
            <PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport> ORPHAN_CHECKER =
            new ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void checkReference( PropertyRecord record, PrimitiveRecord primitiveRecord,
                                            ConsistencyReport.PropertyConsistencyReport report )
                {
                    report.orphanPropertyChain();
                }
            };
}

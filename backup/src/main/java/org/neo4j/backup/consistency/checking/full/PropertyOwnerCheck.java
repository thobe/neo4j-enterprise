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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

class PropertyOwnerCheck
{
    private final Map<Long, PropertyOwner> owners;

    PropertyOwnerCheck( boolean active )
    {
        this.owners = active ? new ConcurrentHashMap<Long, PropertyOwner>( 16, 0.75f, 4 ) : null;
    }

    public <RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport<RECORD, REPORT>>
    RecordCheck<RECORD, REPORT> decoratePrimitiveChecker( final PrimitiveRecordCheck<RECORD, REPORT> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new RecordCheck<RECORD, REPORT>()
        {
            @Override
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

            private PropertyOwner owner( RECORD record )
            {
                if ( record instanceof NodeRecord )
                {
                    return new PropertyOwner.OwningNode( record.getId() );
                }
                else
                {
                    return new PropertyOwner.OwningRelationship( record.getId() );
                }
            }
        };
    }

    public RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        // TODO: add a decorator that allows us to check for orphan chains
        return checker;
    }

    public void scanForOrphanChains( ConsistencyReport.Reporter report, ProgressMonitorFactory progressFactory )
    {
        if ( owners != null )
        {
            ProgressListener progressListener = progressFactory.singlePart( "Checking for orphan property chains", owners.size() );
            for ( PropertyOwner owner : owners.values() )
            {
                owner.checkOphanage( report, REPORT_ORPHAN );
                progressListener.add( 1 );
            }
            progressListener.done();
        }
    }

    private static RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> REPORT_ORPHAN =
            new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                                   RecordAccess records )
                {
                    report.orphanPropertyChain();
                }

                @Override
                public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                         ConsistencyReport.PropertyConsistencyReport report,
                                         DiffRecordAccess records )
                {
                    check( newRecord, report, records );
                }
            };
}

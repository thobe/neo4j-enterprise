package org.neo4j.backup.consistency.checking.full;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.backup.consistency.checking.PrimitiveRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.helpers.Progress;
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
            public void check( RECORD record, REPORT report, RecordReferencer records )
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
            public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordReferencer records )
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

    public void scanForOrphanChains( ConsistencyReport.Reporter report, Progress.Factory progressFactory )
    {
        if ( owners != null )
        {
            Progress progress = progressFactory.singlePart( "Checking for orphan property chains", owners.size() );
            for ( PropertyOwner owner : owners.values() )
            {
                owner.checkOphanage( report, REPORT_ORPHAN );
                progress.add( 1 );
            }
            progress.done();
        }
    }

    private static RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> REPORT_ORPHAN =
            new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                                   RecordReferencer records )
                {
                    report.orphanPropertyChain();
                }

                @Override
                public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                         ConsistencyReport.PropertyConsistencyReport report,
                                         DiffRecordReferencer records )
                {
                    check( newRecord, report, records );
                }
            };
}

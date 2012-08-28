package org.neo4j.backup.consistency.check;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

class PropertyOwnerCheck
{
    private final Map<Long, PropertyOwner> owners;

    PropertyOwnerCheck( boolean active )
    {
        this.owners = active ? new ConcurrentHashMap<Long, PropertyOwner>( 16, 0.75f, 4 ) : null;
    }

    public <RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport<RECORD, REPORT>>
    RecordCheck<RECORD, REPORT> decorate( final PrimitiveRecordCheck<RECORD, REPORT> checker )
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
                        PropertyOwner previous = owners.put( prop, checker.owner( record ) );
                        if ( previous != null )
                        {
                            report.forReference( previous.record( records ), checker.ownerCheck );
                        }
                    }
                }
                checker.check( record, report, records );
            }

            @Override
            public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, RecordReferencer records )
            {
                checker.checkChange( oldRecord, newRecord, report, records );
            }
        };
    }
}

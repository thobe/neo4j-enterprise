package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

abstract class NameRecordCheck<RECORD extends AbstractNameRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        implements RecordCheck<RECORD, REPORT>, ComparativeRecordChecker<RECORD, DynamicRecord, REPORT>
{
    @Override
    public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordReferencer records )
    {
        check( newRecord, report, records );
    }

    @Override
    public void check( RECORD record, REPORT report, RecordReferencer records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        if ( !Record.NO_NEXT_BLOCK.is( record.getNameId() ) )
        {
            report.forReference( name( records, record.getNameId() ), this );
        }
    }

    @Override
    public void checkReference( RECORD record, DynamicRecord name, REPORT report )
    {
        if ( !name.inUse() )
        {
            nameNotInUse( report, name );
        }
        else
        {
            if ( name.getLength() <= 0 )
            {
                emptyName( report, name );
            }
        }
    }

    abstract RecordReference<DynamicRecord> name( RecordReferencer records, int id );

    abstract void nameNotInUse( REPORT report, DynamicRecord name );

    abstract void emptyName( REPORT report, DynamicRecord name );
}

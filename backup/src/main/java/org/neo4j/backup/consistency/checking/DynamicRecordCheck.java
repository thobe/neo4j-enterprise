package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

public class DynamicRecordCheck
        implements RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport>,
        ComparativeRecordChecker<DynamicRecord, DynamicRecord, ConsistencyReport.DynamicConsistencyReport>
{
    public enum StoreDereference
    {
        STRING
        {
            @Override
            RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
            {
                return records.string( block );
            }
        },
        ARRAY
        {
            @Override
            RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
            {
                return records.array( block );
            }
        },
        PROPERTY_KEY
        {
            @Override
            RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
            {
                return records.propertyKeyName( (int) block );
            }
        },
        RELATIONSHIP_LABEL
        {
            @Override
            RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
            {
                return records.relationshipLabelName( (int) block );
            }
        };
        abstract RecordReference<DynamicRecord> lookup(RecordReferencer records, long block);
    }

    private final int blockSize;
    private final StoreDereference dereference;

    public DynamicRecordCheck( RecordStore<DynamicRecord> store, StoreDereference dereference )
    {
        this.blockSize = store.getRecordSize() - store.getRecordHeaderSize();
        this.dereference = dereference;
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                             ConsistencyReport.DynamicConsistencyReport report, DiffRecordReferencer records )
    {
        check( newRecord, report, records );
    }

    @Override
    public void check( DynamicRecord record, ConsistencyReport.DynamicConsistencyReport report, RecordReferencer records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        if ( record.getLength() == 0 )
        {
            report.emptyBlock();
        }
        else if ( record.getLength() < 0 )
        {
            report.invalidLength();
        }
        if ( !Record.NO_NEXT_BLOCK.is( record.getNextBlock() ) )
        {
            if ( record.getNextBlock() == record.getId() )
            {
                report.selfReferentialNext();
            }
            else
            {
                report.forReference( dereference.lookup( records, record.getNextBlock() ), this );
            }
            if ( record.getLength() < blockSize )
            {
                report.recordNotFullReferencesNext();
            }
        }
    }

    @Override
    public void checkReference( DynamicRecord record, DynamicRecord next,
                                ConsistencyReport.DynamicConsistencyReport report )
    {
        if ( !next.inUse() )
        {
            report.nextNotInUse( next );
        }
        else
        {
            if ( next.getLength() <= 0 )
            {
                report.emptyNextBlock( next );
            }
        }
    }
}

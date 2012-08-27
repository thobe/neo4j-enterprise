package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.Record;

public abstract class DynamicRecordCheck
        implements RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport>,
        ComparativeRecordChecker<DynamicRecord, DynamicRecord, ConsistencyReport.DynamicConsistencyReport>
{
    public static class StringRecordCheck extends DynamicRecordCheck
    {
        public StringRecordCheck( DynamicStringStore store )
        {
            super( store.getRecordSize() - store.getRecordHeaderSize() );
        }

        @Override
        RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
        {
            return records.string( block );
        }
    }

    public static class ArrayRecordCheck extends DynamicRecordCheck
    {
        public ArrayRecordCheck( DynamicArrayStore store )
        {
            super( store.getRecordSize() - store.getRecordHeaderSize() );
        }

        @Override
        RecordReference<DynamicRecord> lookup( RecordReferencer records, long block )
        {
            return records.array( block );
        }
    }

    private final int blockSize;

    private DynamicRecordCheck( int blockSize )
    {
        this.blockSize = blockSize;
    }

    @Override
    public ConsistencyReport.DynamicConsistencyReport report( ConsistencyReport.Reporter reporter,
                                                              DynamicRecord record )
    {
        return reporter.forDynamicBlock( record );
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                             ConsistencyReport.DynamicConsistencyReport report, RecordReferencer records )
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
            report.forReference( lookup( records, record.getNextBlock() ), this );
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

    abstract RecordReference<DynamicRecord> lookup( RecordReferencer records, long block );
}

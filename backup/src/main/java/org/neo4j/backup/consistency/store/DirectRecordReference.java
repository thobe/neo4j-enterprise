package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

class DirectRecordReference<RECORD extends AbstractBaseRecord> implements RecordReference<RECORD>
{
    private final RECORD record;
    private final RecordAccess records;

    DirectRecordReference( RECORD record, RecordAccess records )
    {
        this.record = record;
        this.records = records;
    }

    @Override
    public void dispatch( PendingReferenceCheck<RECORD> reporter )
    {
        reporter.checkReference( record, records );
    }
}

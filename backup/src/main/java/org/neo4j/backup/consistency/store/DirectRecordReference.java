package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

class DirectRecordReference<RECORD extends AbstractBaseRecord> implements RecordReference<RECORD>
{
    private final RECORD record;

    DirectRecordReference( RECORD record )
    {
        this.record = record;
    }

    @Override
    public void dispatch( PendingReferenceCheck<RECORD> reporter )
    {
        reporter.checkReference( record );
    }
}

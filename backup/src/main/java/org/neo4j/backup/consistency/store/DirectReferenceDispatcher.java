package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.check.ComparativeRecordChecker;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class DirectReferenceDispatcher implements ReferenceDispatcher
{
    public <RECORD extends AbstractBaseRecord,
            REFERRED extends AbstractBaseRecord,
            REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatch( RECORD record,
                   RecordReference<REFERRED> reference,
                   ComparativeRecordChecker<RECORD, REFERRED, REPORT> checker,
                   REPORT report )
    {
        reference.dispatch( checker, record, report );
    }
}

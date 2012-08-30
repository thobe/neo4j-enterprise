package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface ReferenceDispatcher
{
    public <RECORD extends AbstractBaseRecord,
            REFERRED extends AbstractBaseRecord,
            REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatch( RECORD record,
                   RecordReference<REFERRED> reference,
                   ComparativeRecordChecker<RECORD, REFERRED, REPORT> checker,
                   REPORT report );
}

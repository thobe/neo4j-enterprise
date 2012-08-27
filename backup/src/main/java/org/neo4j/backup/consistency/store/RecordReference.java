package org.neo4j.backup.consistency.store;

import org.neo4j.backup.consistency.check.ComparativeRecordChecker;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface RecordReference<RECORD extends AbstractBaseRecord>
{
    <R extends AbstractBaseRecord, REPORT extends ConsistencyReport<R, REPORT>> void dispatch(
            ComparativeRecordChecker<R, RECORD, REPORT> checker, R record, REPORT report );
}

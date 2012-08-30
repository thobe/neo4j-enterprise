package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface ComparativeRecordChecker<RECORD extends AbstractBaseRecord, REFERRED extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    void checkReference( RECORD record, REFERRED referred, REPORT report );
}

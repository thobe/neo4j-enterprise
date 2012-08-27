package org.neo4j.backup.consistency.check;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface ComparativeRecordChecker<RECORD extends AbstractBaseRecord, REFERRED extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    void checkReference( RECORD record, REFERRED referred, REPORT report );
}

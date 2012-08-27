package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface RecordField<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    void checkConsistency( RECORD record, REPORT report, RecordReferencer records );
}

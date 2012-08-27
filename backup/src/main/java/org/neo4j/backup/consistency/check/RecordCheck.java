package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface RecordCheck<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    REPORT report( ConsistencyReport.Reporter reporter, RECORD record );

    void check( RECORD record, REPORT report, RecordReferencer records );

    void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, RecordReferencer records );
}

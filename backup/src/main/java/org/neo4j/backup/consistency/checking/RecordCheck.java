package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface RecordCheck<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    void check( RECORD record, REPORT report, RecordReferencer records );

    void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordReferencer records );
}

package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

interface RecordField<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    void checkConsistency( RECORD record, REPORT report, RecordReferencer records );

    long valueFrom( RECORD record );

    boolean isNone( RECORD record );

    boolean referencedRecordChanged( DiffRecordReferencer records, RECORD record );

    void reportReplacedButNotUpdated( REPORT report );
}

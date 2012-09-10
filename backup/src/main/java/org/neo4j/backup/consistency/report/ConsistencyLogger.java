package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public interface ConsistencyLogger
{
    void error( RecordType recordType, AbstractBaseRecord record, String message, Object[] args );

    void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord, String message,
                Object[] args );

    void warning( RecordType recordType, AbstractBaseRecord record, String message, Object[] args );

    void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord, String message,
                  Object[] args );
}

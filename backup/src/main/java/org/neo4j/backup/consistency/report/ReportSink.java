package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

class ReportSink implements ConsistencyLogger
{
    private final ConsistencyLogger logger;
    private final ConsistencySummaryStatistics summary;

    ReportSink( ConsistencyLogger logger, ConsistencySummaryStatistics summary )
    {
        this.logger = logger;
        this.summary = summary;
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
    {
        logger.error( recordType, record, message, args );
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                       String message, Object[] args )
    {
        logger.error( recordType, oldRecord, newRecord, message, args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
    {
        logger.warning( recordType, record, message, args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                         String message, Object[] args )
    {
        logger.warning( recordType, oldRecord, newRecord, message, args );
    }

    void updateSummary( RecordType type, int errors, int warnings )
    {
        summary.update( type, errors, warnings );
    }
}

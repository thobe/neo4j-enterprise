package org.neo4j.backup.consistency.report;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.util.StringLogger;

public class MessageConsistencyLogger implements ConsistencyLogger
{
    private final StringLogger logger;
    private static final String ERROR = "ERROR:", WARNING = "WARNING:";

    public MessageConsistencyLogger( StringLogger logger )
    {
        this.logger = logger;
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
    {
        log( record( entry( ERROR, message ), record ), args );
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                       String message, Object... args )
    {
        log( diff( entry( ERROR, message ), oldRecord, newRecord ), args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
    {
        log( record( entry( WARNING, message ), record ), args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                         String message, Object... args )
    {
        log( diff( entry( WARNING, message ), oldRecord, newRecord ), args );
    }

    private static StringBuilder entry( String type, String message )
    {
        StringBuilder log = new StringBuilder( type );
        for ( String line : message.split( "\n" ) )
        {
            log.append( ' ' ).append( line.trim() );
        }
        return log;
    }

    private static StringBuilder record( StringBuilder log, AbstractBaseRecord record )
    {
        return log.append( "\n\t" ).append( record );
    }

    private static StringBuilder diff( StringBuilder log, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord )
    {
        return log.append( "\n\t- " ).append( oldRecord ).append( "\n\t+ " ).append( newRecord );
    }

    private void log( StringBuilder log, Object[] args )
    {
        if ( args != null && args.length > 0 )
        {
            log.append( "\n\tInconsistent with:" );
            for ( Object arg : args )
            {
                log.append( ' ' ).append( arg );
            }
        }
        logger.logMessage( log.toString() );
    }
}

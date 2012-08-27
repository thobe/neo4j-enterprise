package org.neo4j.backup.consistency.report;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.neo4j.backup.consistency.check.ComparativeRecordChecker;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.ReferenceDispatcher;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.reflect.Proxy.newProxyInstance;
import static org.neo4j.helpers.Exceptions.withCause;

public class ConsistencyReporter implements InvocationHandler
{
    private static final Method FOR_REFERENCE;

    static
    {
        try
        {
            FOR_REFERENCE = ConsistencyReport.class
                    .getDeclaredMethod( "forReference", RecordReference.class,
                                        ComparativeRecordChecker.class );
        }
        catch ( NoSuchMethodException cause )
        {
            throw withCause(
                    new LinkageError( "Could not find dispatch method of " + ConsistencyReport.class.getName() ),
                    cause );
        }
    }

    private final ReferenceDispatcher dispatcher;
    private final StringLogger logger;
    private final AbstractBaseRecord record;

    private ConsistencyReporter( ReferenceDispatcher dispatcher, StringLogger logger,
                                 AbstractBaseRecord record )
    {
        this.dispatcher = dispatcher;
        this.logger = logger;
        this.record = record;
    }

    public static ConsistencyReport.Reporter create( final ReferenceDispatcher dispatcher, final StringLogger logger )
    {
        return proxy( ConsistencyReport.Reporter.class, new InvocationHandler()
        {
            @Override
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                return proxy( method.getReturnType(),
                              new ConsistencyReporter( dispatcher, logger, (AbstractBaseRecord) args[0] ) );
            }
        } );
    }

    @Override
    public synchronized Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        if ( method.equals( FOR_REFERENCE ) )
        {
            dispatchForReference( (ConsistencyReport) proxy, args );
        }
        else
        {
            StringBuilder message = new StringBuilder(
                    method.getAnnotation( ConsistencyReport.Warning.class ) == null ? "ERROR: " : "WARNING: " );
            message.append( record ).append( ' ' );
            if ( args != null )
            {
                for ( Object arg : args )
                {
                    message.append( arg ).append( ' ' );
                }
            }
            Documented annotation = method.getAnnotation( Documented.class );
            if ( annotation != null )
            {
                message.append( "// " ).append( annotation.value() );
            }
            logger.logMessage( message.toString() );
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void dispatchForReference( ConsistencyReport proxy, Object[] args )
    {
        dispatcher.dispatch( record, (RecordReference) args[0], (ComparativeRecordChecker) args[1], proxy );
    }

    private static <T> T proxy( Class<T> type, InvocationHandler handler )
    {
        return type.cast( newProxyInstance( ConsistencyReporter.class.getClassLoader(), new Class[]{type}, handler ) );
    }
}

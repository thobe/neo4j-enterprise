package org.neo4j.backup.consistency.report;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.neo4j.backup.consistency.check.ComparativeRecordChecker;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.backup.consistency.check.RecordCheck;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.backup.consistency.store.ReferenceDispatcher;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
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

    public static ConsistencyReport.Reporter create( RecordAccess access, final ReferenceDispatcher dispatcher,
                                                     final StringLogger logger )
    {
        final RecordReferencer records = new RecordReferencer( access );
        return new ConsistencyReport.Reporter()
        {
            private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
            REPORT reporter( Class<REPORT> type, RECORD record )
            {
                return proxy( type, new ConsistencyReporter( dispatcher, logger, record ) );
            }

            @Override
            public void forNode( NodeRecord node,
                                 RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
            {
                checker.check( node, reporter( ConsistencyReport.NodeConsistencyReport.class, node ), records );
            }

            @Override
            public void forRelationship( RelationshipRecord relationship,
                                         RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
            {
                checker.check( relationship,
                               reporter( ConsistencyReport.RelationshipConsistencyReport.class, relationship ),
                               records );
            }

            @Override
            public void forProperty( PropertyRecord property,
                                     RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
            {
                checker.check( property, reporter( ConsistencyReport.PropertyConsistencyReport.class, property ),
                               records );
            }

            @Override
            public void forRelationshipLabel( RelationshipTypeRecord label,
                                              RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
            {
                checker.check( label, reporter( ConsistencyReport.LabelConsistencyReport.class, label ), records );
            }

            @Override
            public void forPropertyKey( PropertyIndexRecord key,
                                        RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
            {
                checker.check( key, reporter( ConsistencyReport.PropertyKeyConsistencyReport.class, key ), records );
            }

            @Override
            public void forDynamicBlock( DynamicRecord record,
                                         RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
            {
                checker.check( record, reporter( ConsistencyReport.DynamicConsistencyReport.class, record ), records );
            }
        };
    }

    /**
     * Invoked when an inconsistency is encountered.
     * @param args array of the items referenced from this record with which it is inconsistent.
     */
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
            if ( annotation != null && !"".equals( annotation.value() ) )
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

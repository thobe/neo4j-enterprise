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
    private int errors, warnings;

    private ConsistencyReporter( ReferenceDispatcher dispatcher, StringLogger logger, AbstractBaseRecord record )
    {
        this.dispatcher = dispatcher;
        this.logger = logger;
        this.record = record;
    }

    public static SummarisingReporter create( RecordAccess access, final ReferenceDispatcher dispatcher,
                                              final StringLogger logger )
    {
        return new SummarisingReporter( dispatcher, logger, new RecordReferencer( access ) );
    }

    private void update( ConsistencySummaryStats summary )
    {
        summary.add( record.getClass(), errors, warnings );
    }

    /**
     * Invoked when an inconsistency is encountered.
     *
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
            StringBuilder message = new StringBuilder();
            if ( method.getAnnotation( ConsistencyReport.Warning.class ) == null )
            {
                errors++;
                message.append( "ERROR: " );
            }
            else
            {
                warnings++;
                message.append( "WARNING: " );
            }
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

    public static class SummarisingReporter implements ConsistencyReport.Reporter
    {
        private final ReferenceDispatcher dispatcher;
        private final StringLogger logger;
        private final RecordReferencer records;
        private final ConsistencySummaryStats summary = new ConsistencySummaryStats();

        private SummarisingReporter( ReferenceDispatcher dispatcher, StringLogger logger, RecordReferencer records )
        {
            this.dispatcher = dispatcher;
            this.logger = logger;
            this.records = records;
        }

        public ConsistencySummaryStats getSummary()
        {
            return summary;
        }

        private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD,REPORT>>
        void dispatch(Class<REPORT> reportType, RECORD record, RecordCheck<RECORD,REPORT> checker)
        {
            ConsistencyReporter handler = new ConsistencyReporter( dispatcher, logger, record );
            checker.check( record, proxy( reportType, handler ), records );
            handler.update( summary );
        }

        @Override
        public void forNode( NodeRecord node,
                             RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.NodeConsistencyReport.class, node, checker );
        }

        @Override
        public void forRelationship( RelationshipRecord relationship,
                                     RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.RelationshipConsistencyReport.class, relationship, checker );
        }

        @Override
        public void forProperty( PropertyRecord property,
                                 RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.PropertyConsistencyReport.class, property, checker );
        }

        @Override
        public void forRelationshipLabel( RelationshipTypeRecord label,
                                          RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.LabelConsistencyReport.class, label, checker );
        }

        @Override
        public void forPropertyKey( PropertyIndexRecord key,
                                    RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.PropertyKeyConsistencyReport.class, key, checker );
        }

        @Override
        public void forDynamicBlock( DynamicRecord record,
                                     RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            dispatch( ConsistencyReport.DynamicConsistencyReport.class, record, checker );
        }
    }
}

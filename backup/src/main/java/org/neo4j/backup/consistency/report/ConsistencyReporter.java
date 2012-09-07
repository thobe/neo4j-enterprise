/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.consistency.report;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.reflect.Proxy.getInvocationHandler;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Exceptions.withCause;

public class ConsistencyReporter implements ConsistencyReport.Reporter
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
            throw withCause( new LinkageError( "Could not find dispatch method of " +
                                               ConsistencyReport.class.getName() ), cause );
        }
    }

    private static final ProxyFactory<ConsistencyReport.NodeConsistencyReport> NODE_REPORT =
            ProxyFactory.create( ConsistencyReport.NodeConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.RelationshipConsistencyReport> RELATIONSHIP_REPORT =
            ProxyFactory.create( ConsistencyReport.RelationshipConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.PropertyConsistencyReport> PROPERTY_REPORT =
            ProxyFactory.create( ConsistencyReport.PropertyConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.LabelConsistencyReport> LABEL_REPORT =
            ProxyFactory.create( ConsistencyReport.LabelConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.PropertyKeyConsistencyReport> PROPERTY_KEY_REPORT =
            ProxyFactory.create( ConsistencyReport.PropertyKeyConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.DynamicConsistencyReport> DYNAMIC_REPORT =
            ProxyFactory.create( ConsistencyReport.DynamicConsistencyReport.class );

    private final StringLogger logger;
    private final DiffRecordAccess records;
    private final ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();

    public ConsistencyReporter( StringLogger logger, DiffRecordAccess records )
    {
        this.logger = logger;
        this.records = records;
    }

    public ConsistencySummaryStatistics getSummary()
    {
        return summary;
    }

    private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatch( RecordType type, ProxyFactory<REPORT> factory, RECORD record, RecordCheck<RECORD, REPORT> checker )
    {
        ReportHandler handler = new ReportHandler( logger, summary, type, record );
        checker.check( record, factory.create( handler ), records );
        handler.updateSummary();
    }

    private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatchChange( RecordType type, ProxyFactory<REPORT> factory, RECORD oldRecord, RECORD newRecord,
                         RecordCheck<RECORD, REPORT> checker )
    {
        DiffReportHandler handler = new DiffReportHandler( logger, summary, type, oldRecord, newRecord );
        checker.checkChange( oldRecord, newRecord, factory.create( handler ), records );
        handler.updateSummary();
    }

    static void dispatchReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                   AbstractBaseRecord referenced )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) getInvocationHandler( report );
        handler.checkReference( report, checker, referenced );
        handler.updateSummary();
    }

    static void dispatchChangeReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                         AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) getInvocationHandler( report );
        handler.checkDiffReference( report, checker, oldReferenced, newReferenced );
        handler.updateSummary();
    }

    static void dispatchSkip( ConsistencyReport report )
    {
        ((ReportInvocationHandler) getInvocationHandler( report )).updateSummary();
    }

    private static abstract class ReportInvocationHandler implements InvocationHandler
    {
        private final StringLogger logger;
        private final ConsistencySummaryStatistics summary;
        private final RecordType type;
        private short errors = 0, warnings = 0, references = 1/*this*/;

        private ReportInvocationHandler( StringLogger logger, ConsistencySummaryStatistics summary, RecordType type )
        {
            this.logger = logger;
            this.summary = summary;
            this.type = type;
        }

        synchronized void updateSummary()
        {
            if ( --references == 0 )
            {
                summary.add( type, errors, warnings );
            }
        }

        /**
         * Invoked when an inconsistency is encountered.
         *
         * @param args array of the items referenced from this record with which it is inconsistent.
         */
        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( method.equals( FOR_REFERENCE ) )
            {
                RecordReference reference = (RecordReference) args[0];
                ComparativeRecordChecker checker = (ComparativeRecordChecker) args[1];
                dispatchForReference( (ConsistencyReport) proxy, reference, checker );
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
                emitRecord( message );
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
        private void dispatchForReference( ConsistencyReport report, RecordReference reference,
                                   ComparativeRecordChecker checker )
        {
            forReference( report, reference, checker );
        }

        final <REFERENCED extends AbstractBaseRecord>
        void forReference( ConsistencyReport report, RecordReference<REFERENCED> reference,
                           ComparativeRecordChecker<?, REFERENCED, ?> checker )
        {
            references++;
            reference.dispatch( new ReportReference<REFERENCED>( report, checker ) );
        }

        abstract void emitRecord( StringBuilder message );

        abstract void checkReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                      AbstractBaseRecord referenced );

        abstract void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                          AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced );
    }

    static class ReportHandler extends ReportInvocationHandler
    {
        private final AbstractBaseRecord record;

        ReportHandler( StringLogger logger, ConsistencySummaryStatistics summary, RecordType type,
                       AbstractBaseRecord record )
        {
            super( logger, summary, type );
            this.record = record;
        }

        @Override
        void emitRecord( StringBuilder message )
        {
            message.append( record ).append( ' ' );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkReference( ConsistencyReport report, ComparativeRecordChecker checker, AbstractBaseRecord referenced )
        {
            checker.checkReference( record, referenced, report );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                 AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced )
        {
            checker.checkReference( record, newReferenced, report );
        }
    }

    private static class DiffReportHandler extends ReportInvocationHandler
    {
        private final AbstractBaseRecord oldRecord;
        private final AbstractBaseRecord newRecord;

        private DiffReportHandler( StringLogger logger, ConsistencySummaryStatistics summary,
                                   RecordType type, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord )
        {
            super( logger, summary, type );
            this.oldRecord = oldRecord;
            this.newRecord = newRecord;
        }

        @Override
        void emitRecord( StringBuilder message )
        {
            message.append( newRecord ).append( ' ' );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkReference( ConsistencyReport report, ComparativeRecordChecker checker, AbstractBaseRecord referenced )
        {
            checker.checkReference( newRecord, referenced, report );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                 AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced )
        {
            checker.checkReference( newRecord, newReferenced, report );
        }
    }

    @Override
    public void forNode( NodeRecord node,
                         RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        dispatch( RecordType.NODE, NODE_REPORT, node, checker );
    }

    @Override
    public void forNodeChange( NodeRecord oldNode, NodeRecord newNode,
                               RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        dispatchChange( RecordType.NODE, NODE_REPORT, oldNode, newNode, checker );
    }

    @Override
    public void forRelationship( RelationshipRecord relationship,
                                 RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        dispatch( RecordType.RELATIONSHIP, RELATIONSHIP_REPORT, relationship, checker );
    }

    @Override
    public void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship,
                                       RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        dispatchChange( RecordType.RELATIONSHIP, RELATIONSHIP_REPORT, oldRelationship, newRelationship, checker );
    }

    @Override
    public void forProperty( PropertyRecord property,
                             RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        dispatch( RecordType.PROPERTY, PROPERTY_REPORT, property, checker );
    }

    @Override
    public void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                   RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        dispatchChange( RecordType.PROPERTY, PROPERTY_REPORT, oldProperty, newProperty, checker );
    }

    @Override
    public void forRelationshipLabel( RelationshipTypeRecord label,
                                      RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
    {
        dispatch( RecordType.RELATIONSHIP_LABEL, LABEL_REPORT, label, checker );
    }

    @Override
    public void forRelationshipLabelChange( RelationshipTypeRecord oldLabel, RelationshipTypeRecord newLabel,
                                            RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
    {
        dispatchChange( RecordType.RELATIONSHIP_LABEL, LABEL_REPORT, oldLabel, newLabel, checker );
    }

    @Override
    public void forPropertyKey( PropertyIndexRecord key,
                                RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
    {
        dispatch( RecordType.PROPERTY_KEY, PROPERTY_KEY_REPORT, key, checker );
    }

    @Override
    public void forPropertyKeyChange( PropertyIndexRecord oldKey, PropertyIndexRecord newKey,
                                      RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
    {
        dispatchChange( RecordType.PROPERTY_KEY, PROPERTY_KEY_REPORT, oldKey, newKey, checker );
    }

    @Override
    public void forDynamicBlock( RecordType type, DynamicRecord record,
                                 RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        dispatch( type, DYNAMIC_REPORT, record, checker );
    }

    @Override
    public void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                       RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        dispatchChange( type, DYNAMIC_REPORT, oldRecord, newRecord, checker );
    }

    private static class ProxyFactory<T>
    {
        private Constructor<? extends T> constructor;

        @SuppressWarnings("unchecked")
        ProxyFactory( Class<T> type ) throws LinkageError
        {
            try
            {
                this.constructor = (Constructor<? extends T>) Proxy
                        .getProxyClass( ConsistencyReporter.class.getClassLoader(), type )
                        .getConstructor( InvocationHandler.class );
            }
            catch ( NoSuchMethodException e )
            {
                throw withCause( new LinkageError( "Cannot access Proxy constructor for " + type.getName() ), e );
            }
        }

        public T create( InvocationHandler handler )
        {
            try
            {
                return constructor.newInstance( handler );
            }
            catch ( InvocationTargetException e )
            {
                throw launderedException( e );
            }
            catch ( Exception e )
            {
                throw new LinkageError( "Failed to create proxy instance" );
            }
        }

        public static <T> ProxyFactory<T> create( Class<T> type )
        {
            return new ProxyFactory<T>( type );
        }
    }
}

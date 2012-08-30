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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
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

    private final RecordType type;
    private final ReferenceDispatcher dispatcher;
    private final StringLogger logger;
    private final AbstractBaseRecord record;
    private int errors, warnings;

    private ConsistencyReporter( RecordType type, ReferenceDispatcher dispatcher, StringLogger logger,
                                 AbstractBaseRecord record )
    {
        this.type = type;
        this.dispatcher = dispatcher;
        this.logger = logger;
        this.record = record;
    }

    public static SummarisingReporter create( RecordAccess access, final ReferenceDispatcher dispatcher,
                                              final StringLogger logger )
    {
        return new SummarisingReporter( dispatcher, logger, new DiffRecordReferencer( access ) );
    }

    private void update( ConsistencySummaryStatistics summary )
    {
        summary.add( type, errors, warnings );
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
        private final DiffRecordReferencer records;
        private final ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();

        private SummarisingReporter( ReferenceDispatcher dispatcher, StringLogger logger, DiffRecordReferencer records )
        {
            this.dispatcher = dispatcher;
            this.logger = logger;
            this.records = records;
        }

        public ConsistencySummaryStatistics getSummary()
        {
            return summary;
        }

        private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        void dispatch( RecordType type, Class<REPORT> reportType, RECORD record, RecordCheck<RECORD, REPORT> checker )
        {
            ConsistencyReporter handler = new ConsistencyReporter( type, dispatcher, logger, record );
            checker.check( record, proxy( reportType, handler ), records );
            handler.update( summary );
        }

        private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        void dispatchChange( RecordType type, Class<REPORT> reportType, RECORD oldRecord, RECORD newRecord,
                             RecordCheck<RECORD, REPORT> checker )
        {
            ConsistencyReporter handler = new ConsistencyReporter( type, dispatcher, logger, newRecord );
            checker.checkChange( oldRecord, newRecord, proxy( reportType, handler ), records );
            handler.update( summary );
        }

        @Override
        public void forNode( NodeRecord node,
                             RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            dispatch( RecordType.NODE, ConsistencyReport.NodeConsistencyReport.class, node, checker );
        }

        @Override
        public void forNodeChange( NodeRecord oldNode, NodeRecord newNode,
                                   RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            dispatchChange( RecordType.NODE, ConsistencyReport.NodeConsistencyReport.class, oldNode, newNode, checker );
        }

        @Override
        public void forRelationship( RelationshipRecord relationship,
                                     RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            dispatch( RecordType.RELATIONSHIP, ConsistencyReport.RelationshipConsistencyReport.class, relationship,
                      checker );
        }

        @Override
        public void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship,
                                           RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            dispatchChange( RecordType.RELATIONSHIP, ConsistencyReport.RelationshipConsistencyReport.class,
                            oldRelationship, newRelationship, checker );
        }

        @Override
        public void forProperty( PropertyRecord property,
                                 RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            dispatch( RecordType.PROPERTY, ConsistencyReport.PropertyConsistencyReport.class, property, checker );
        }

        @Override
        public void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                       RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            dispatchChange( RecordType.PROPERTY, ConsistencyReport.PropertyConsistencyReport.class, oldProperty,
                            newProperty, checker );
        }

        @Override
        public void forRelationshipLabel( RelationshipTypeRecord label,
                                          RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            dispatch( RecordType.RELATIONSHIP_LABEL, ConsistencyReport.LabelConsistencyReport.class, label, checker );
        }

        @Override
        public void forRelationshipLabelChange( RelationshipTypeRecord oldLabel, RelationshipTypeRecord newLabel,
                                                RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            dispatchChange( RecordType.RELATIONSHIP_LABEL, ConsistencyReport.LabelConsistencyReport.class, oldLabel,
                            newLabel, checker );
        }

        @Override
        public void forPropertyKey( PropertyIndexRecord key,
                                    RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            dispatch( RecordType.PROPERTY_KEY, ConsistencyReport.PropertyKeyConsistencyReport.class, key, checker );
        }

        @Override
        public void forPropertyKeyChange( PropertyIndexRecord oldKey, PropertyIndexRecord newKey,
                                          RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            dispatchChange( RecordType.PROPERTY_KEY, ConsistencyReport.PropertyKeyConsistencyReport.class, oldKey,
                            newKey, checker );
        }

        @Override
        public void forDynamicBlock( RecordType type, DynamicRecord record,
                                     RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            dispatch( type, ConsistencyReport.DynamicConsistencyReport.class, record, checker );
        }

        @Override
        public void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                           RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            dispatchChange( type, ConsistencyReport.DynamicConsistencyReport.class, oldRecord, newRecord, checker );
        }
    }
}

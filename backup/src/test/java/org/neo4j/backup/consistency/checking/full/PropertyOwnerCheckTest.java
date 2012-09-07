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
package org.neo4j.backup.consistency.checking.full;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.NodeRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.checking.RelationshipRecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordAccessStub;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.NONE;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.check;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.notInUse;

public class PropertyOwnerCheckTest
{
    @Test
    public void shouldNotDecorateCheckerWhenInactive() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( false );
        NodeRecordCheck checker = new NodeRecordCheck();

        // when
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorated = decorator.decoratePrimitiveChecker(
                checker );

        // then
        assertSame( checker, decorated );
    }

    @Test
    public void shouldNotReportAnythingForNodesWithDifferentPropertyChains() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decoratePrimitiveChecker( nodeChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = records.add( inUse( new NodeRecord( 2, NONE, 8 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldNotReportAnythingForNodesNotInUse() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decoratePrimitiveChecker( nodeChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( notInUse( new NodeRecord( 1, NONE, 6 ) ) );
        NodeRecord node2 = records.add( notInUse( new NodeRecord( 2, NONE, 6 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipsWithDifferentPropertyChains() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decoratePrimitiveChecker( relationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord relationship1 = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = records.add( inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
        relationship2.setNextProp( 8 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report1 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship1, records );
        ConsistencyReport.RelationshipConsistencyReport report2 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship2, records );

        // then
        verifyZeroInteractions( report1 );
        verifyZeroInteractions( report2 );
    }

    @Test
    public void shouldReportTwoNodesWithSamePropertyChain() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decoratePrimitiveChecker( nodeChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node1 = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = records.add( inUse( new NodeRecord( 2, NONE, 7 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report1 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node1, records );
        ConsistencyReport.NodeConsistencyReport report2 =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node2, records );

        // then
        verifyZeroInteractions( report1 );
        verify( report2 ).multipleOwners( node1 );
    }

    @Test
    public void shouldReportTwoRelationshipsWithSamePropertyChain() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decoratePrimitiveChecker( relationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord relationship1 = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = records.add( inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
        relationship2.setNextProp( relationship1.getNextProp() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report1 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship1, records );
        ConsistencyReport.RelationshipConsistencyReport report2 =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship2, records );

        // then
        verifyZeroInteractions( report1 );
        verify( report2 ).multipleOwners( relationship1 );
    }

    @Test
    public void shouldReportRelationshipWithSamePropertyChainAsNode() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decoratePrimitiveChecker( nodeChecker() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decoratePrimitiveChecker( relationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );

        // then
        verifyZeroInteractions( nodeReport );
        verify( relationshipReport ).multipleOwners( node );
    }

    @Test
    public void shouldReportNodeWithSamePropertyChainAsRelationship() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decoratePrimitiveChecker( nodeChecker() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decoratePrimitiveChecker( relationshipChecker() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );

        // then
        verifyZeroInteractions( relationshipReport );
        verify( nodeReport ).multipleOwners( relationship );
    }

    @Ignore("implement this capability after deleting the old checker code")
    @Test
    public void shouldReportOrphanPropertyChain() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker = decorator
                .decoratePropertyChecker( propertyChecker() );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport originalReport = check(
                ConsistencyReport.PropertyConsistencyReport.class, checker, record, new RecordAccessStub() );
        ConsistencyReport.PropertyConsistencyReport report = mock( ConsistencyReport.PropertyConsistencyReport.class );

        // when
        decorator.scanForOrphanChains( new StubOrphanReporter( report ), ProgressMonitorFactory.NONE );

        // then
        verifyZeroInteractions( originalReport );
        verify( report ).orphanPropertyChain();
        verifyNoMoreInteractions( report );
    }

    private static NodeRecordCheck nodeChecker()
    {
        return new NodeRecordCheck()
        {
            @Override
            public void check( NodeRecord record, ConsistencyReport.NodeConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     ConsistencyReport.NodeConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    private static RelationshipRecordCheck relationshipChecker()
    {
        return new RelationshipRecordCheck()
        {
            @Override
            public void check( RelationshipRecord record, ConsistencyReport.RelationshipConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                     ConsistencyReport.RelationshipConsistencyReport report,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    private static RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyChecker()
    {
        return new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     ConsistencyReport.PropertyConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    private static class StubOrphanReporter implements ConsistencyReport.Reporter
    {
        private final ConsistencyReport.PropertyConsistencyReport report;

        StubOrphanReporter( ConsistencyReport.PropertyConsistencyReport report )
        {
            this.report = report;
        }

        @Override
        public void forNode( NodeRecord node,
                             RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forNodeChange( NodeRecord oldNode, NodeRecord newNode,
                                   RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forRelationship( RelationshipRecord relationship,
                                     RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship,
                                           RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forProperty( PropertyRecord property,
                                 RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            checker.check( property, report, mock( RecordAccess.class ) );
        }

        @Override
        public void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                       RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forRelationshipLabel( RelationshipTypeRecord label,
                                          RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forRelationshipLabelChange( RelationshipTypeRecord oldLabel, RelationshipTypeRecord newLabel,
                                                RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forPropertyKey( PropertyIndexRecord key,
                                    RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forPropertyKeyChange( PropertyIndexRecord oldKey, PropertyIndexRecord newKey,
                                          RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forDynamicBlock( RecordType type, DynamicRecord record,
                                     RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                           RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}

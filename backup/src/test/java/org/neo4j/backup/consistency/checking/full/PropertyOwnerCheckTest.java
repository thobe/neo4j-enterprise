package org.neo4j.backup.consistency.checking.full;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.NodeRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.checking.RelationshipRecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReferencer;
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
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.add;
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

        RecordAccess records = mock( RecordAccess.class );

        NodeRecord node1 = add( records, inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = add( records, inUse( new NodeRecord( 2, NONE, 8 ) ) );

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

        RecordAccess records = mock( RecordAccess.class );

        NodeRecord node1 = add( records, notInUse( new NodeRecord( 1, NONE, 6 ) ) );
        NodeRecord node2 = add( records, notInUse( new NodeRecord( 2, NONE, 6 ) ) );

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

        RecordAccess records = mock( RecordAccess.class );

        RelationshipRecord relationship1 = add( records, inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = add( records, inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
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

        RecordAccess records = mock( RecordAccess.class );

        NodeRecord node1 = add( records, inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NodeRecord node2 = add( records, inUse( new NodeRecord( 2, NONE, 7 ) ) );

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

        RecordAccess records = mock( RecordAccess.class );

        RelationshipRecord relationship1 = add( records, inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship1.setNextProp( 7 );
        RelationshipRecord relationship2 = add( records, inUse( new RelationshipRecord( 2, 0, 1, 0 ) ) );
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

        RecordAccess records = mock( RecordAccess.class );

        NodeRecord node = add( records, inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
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

        RecordAccess records = mock( RecordAccess.class );

        NodeRecord node = add( records, inUse( new NodeRecord( 1, NONE, 7 ) ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
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
                ConsistencyReport.PropertyConsistencyReport.class, checker, record, mock( RecordAccess.class ) );
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
                               RecordReferencer records )
            {
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     ConsistencyReport.NodeConsistencyReport report, DiffRecordReferencer records )
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
                               RecordReferencer records )
            {
            }

            @Override
            public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                     ConsistencyReport.RelationshipConsistencyReport report,
                                     DiffRecordReferencer records )
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
                               RecordReferencer records )
            {
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     ConsistencyReport.PropertyConsistencyReport report, DiffRecordReferencer records )
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
        public void forRelationship( RelationshipRecord relationship,
                                     RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public void forProperty( PropertyRecord property,
                                 RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            checker.check( property, report, mock( RecordReferencer.class ) );
        }

        @Override
        public void forRelationshipLabel( RelationshipTypeRecord label,
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
        public void forDynamicBlock( RecordType type, DynamicRecord record,
                                     RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}

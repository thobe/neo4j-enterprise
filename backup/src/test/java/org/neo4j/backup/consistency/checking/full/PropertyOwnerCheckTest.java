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

import org.junit.Test;
import org.neo4j.backup.consistency.checking.NeoStoreCheck;
import org.neo4j.backup.consistency.checking.NodeRecordCheck;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.checking.RelationshipRecordCheck;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordAccessStub;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.NONE;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.check;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.backup.consistency.checking.RecordCheckTestBase.verifyOnlyReferenceDispatch;

public class PropertyOwnerCheckTest
{
    @Test
    public void shouldNotDecorateCheckerWhenInactive() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( false );
        NodeRecordCheck checker = new NodeRecordCheck();

        // when
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorated =
                decorator.decorateNodeChecker( checker );

        // then
        assertSame( checker, decorated );
    }

    @Test
    public void shouldNotReportAnythingForNodesWithDifferentPropertyChains() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( nodeChecker() );

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
                decorator.decorateNodeChecker( nodeChecker() );

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
                decorator.decorateRelationshipChecker( relationshipChecker() );

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
                decorator.decorateNodeChecker( nodeChecker() );

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
                decorator.decorateRelationshipChecker( relationshipChecker() );

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
                decorator.decorateNodeChecker( nodeChecker() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( relationshipChecker() );

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
    public void shouldReportRelationshipWithReferenceToTheGraphGlobalChain() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( relationshipChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( neoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( 7 );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( 7 );

        // when
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );

        // then
        verifyZeroInteractions( masterReport );
        verify( relationshipReport ).multipleOwners( master );
    }

    @Test
    public void shouldReportNodeWithSamePropertyChainAsRelationship() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( nodeChecker() );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( relationshipChecker() );

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

    @Test
    public void shouldReportNodeWithReferenceToTheGraphGlobalChain() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( nodeChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( neoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );

        // then
        verifyZeroInteractions( masterReport );
        verify( nodeReport ).multipleOwners( master );
    }

    @Test
    public void shouldReportNodeStoreReferencingSameChainAsNode() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker =
                decorator.decorateNodeChecker( nodeChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( neoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NodeRecord node = records.add( inUse( new NodeRecord( 1, NONE, 7 ) ) );
        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( node.getNextProp() );

        // when
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class, nodeChecker, node, records );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );

        // then
        verifyZeroInteractions( nodeReport );
        verify( masterReport ).multipleOwners( node );
    }

    @Test
    public void shouldReportNodeStoreReferencingSameChainAsRelationship() throws Exception
    {
        // given
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker =
                decorator.decorateRelationshipChecker( relationshipChecker() );
        RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreCheck =
                decorator.decorateNeoStoreChecker( neoStoreCheck() );

        RecordAccessStub records = new RecordAccessStub();

        NeoStoreRecord master = records.add( new NeoStoreRecord() );
        master.setNextProp( 7 );
        RelationshipRecord relationship = records.add( inUse( new RelationshipRecord( 1, 0, 1, 0 ) ) );
        relationship.setNextProp( 7 );

        // when
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       relationshipChecker, relationship, records );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class, neoStoreCheck, master, records );

        // then
        verifyZeroInteractions( relationshipReport );
        verify( masterReport ).multipleOwners( relationship );
    }

    @Test
    public void shouldReportOrphanPropertyChain() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );
        RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker = decorator
                .decoratePropertyChecker( propertyChecker() );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report = check(
                ConsistencyReport.PropertyConsistencyReport.class, checker, record, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verify( report ).orphanPropertyChain();
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByNode() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( propertyChecker() ),
                       record, records );
        ConsistencyReport.NodeConsistencyReport nodeReport =
                check( ConsistencyReport.NodeConsistencyReport.class,
                       decorator.decorateNodeChecker( nodeChecker() ),
                       inUse( new NodeRecord( 10, NONE, 42 ) ), records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( nodeReport );
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByRelationship() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( propertyChecker() ),
                       record, records );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 10, 1, 1, 0 ) );
        relationship.setNextProp( 42 );
        ConsistencyReport.RelationshipConsistencyReport relationshipReport =
                check( ConsistencyReport.RelationshipConsistencyReport.class,
                       decorator.decorateRelationshipChecker( relationshipChecker() ),
                       relationship, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( relationshipReport );
    }

    @Test
    public void shouldNotReportOrphanIfOwnedByNeoStore() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();
        PropertyOwnerCheck decorator = new PropertyOwnerCheck( true );

        PropertyRecord record = inUse( new PropertyRecord( 42 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                check( ConsistencyReport.PropertyConsistencyReport.class,
                       decorator.decoratePropertyChecker( propertyChecker() ),
                       record, records );
        NeoStoreRecord master = inUse( new NeoStoreRecord() );
        master.setNextProp( 42 );
        ConsistencyReport.NeoStoreConsistencyReport masterReport =
                check( ConsistencyReport.NeoStoreConsistencyReport.class,
                       decorator.decorateNeoStoreChecker( neoStoreCheck() ),
                       master, records );

        // when
        decorator.scanForOrphanChains( ProgressMonitorFactory.NONE );

        records.checkDeferred();

        // then
        verifyOnlyReferenceDispatch( report );
        verifyOnlyReferenceDispatch( masterReport );
    }

    private static NeoStoreCheck neoStoreCheck()
    {
        return new NeoStoreCheck()
        {
            @Override
            public void check( NeoStoreRecord record, ConsistencyReport.NeoStoreConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NeoStoreRecord oldRecord, NeoStoreRecord newRecord,
                                     ConsistencyReport.NeoStoreConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
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
}

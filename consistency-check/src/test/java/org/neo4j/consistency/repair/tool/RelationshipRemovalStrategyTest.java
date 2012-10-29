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
package org.neo4j.consistency.repair.tool;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;

import org.junit.Test;
import org.neo4j.consistency.repair.RecordSet;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class RelationshipRemovalStrategyTest
{
    @Test
    public void shouldAdviseRemovalOfUnconnectedRelationshipRecords() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord inconsistentRecord1 = records.add( inUse( new RelationshipRecord( 11, 101, 102, 0 ) ) );
        RelationshipRecord inconsistentRecord2 = records.add( inUse( new RelationshipRecord( 12, 103, 104, 0 ) ) );
        records.add( notInUse( new NodeRecord( 101, -1, -1 ) ) );
        records.add( notInUse( new NodeRecord( 102, -1, -1 ) ) );
        records.add( notInUse( new NodeRecord( 103, -1, -1 ) ) );
        records.add( notInUse( new NodeRecord( 104, -1, -1 ) ) );

        RecordRemover remover = mock( RecordRemover.class );
        Diagnostics diagnostics = mock( Diagnostics.class );

        RelationshipRemovalStrategy removalStrategy = new RelationshipRemovalStrategy( records, remover, diagnostics );

        // when
        removalStrategy.evaluate( asList( inconsistentRecord1, inconsistentRecord2 ) );

        // then
        verify( remover ).remove( inconsistentRecord1 );
        verify( remover ).remove( inconsistentRecord2 );
    }

    @Test
    public void shouldAdviseRemovalOfInconsistentRelationshipRecordAndConnectedConsistentRecords() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord inconsistentRecord1 = records.add( inUse( new RelationshipRecord( 11, 101, 102, 0 ) ) );
        inconsistentRecord1.setFirstNextRel( 10 );
        RelationshipRecord inconsistentRecord2 = records.add( inUse( new RelationshipRecord( 12, 103, 104, 0 ) ) );
        inconsistentRecord2.setSecondNextRel( 10 );
        RelationshipRecord consistentRecord = records.add( inUse( new RelationshipRecord( 10, 101, 104, 0 ) ) );
        consistentRecord.setFirstPrevRel( 11 );
        consistentRecord.setSecondPrevRel( 12 );

        records.add( inUse( new NodeRecord( 101, 1001, -1 ) ) );
        records.add( inUse( new NodeRecord( 102, 1002, -1 ) ) );
        records.add( inUse( new NodeRecord( 103, 1003, -1 ) ) );
        records.add( inUse( new NodeRecord( 104, 1004, -1 ) ) );

        RecordRemover remover = mock( RecordRemover.class );
        Diagnostics diagnostics = mock( Diagnostics.class );

        RelationshipRemovalStrategy removalStrategy = new RelationshipRemovalStrategy( records, remover, diagnostics );

        // when
        removalStrategy.evaluate( asList( inconsistentRecord1, inconsistentRecord2 ) );

        // then
        verify( remover ).remove( inconsistentRecord1 );
        verify( remover ).remove( inconsistentRecord2 );
        verify( remover ).remove( consistentRecord );
    }

    @Test
    public void shouldReportInabilityToRemoveInconsistentRecordsIfTheyReferToAConsistentRecordThatCannotBeRemoved() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord inconsistentRecord1 = records.add( inUse( new RelationshipRecord( 11, 101, 102, 0 ) ) );
        inconsistentRecord1.setFirstNextRel( 10 );
        RelationshipRecord inconsistentRecord2 = records.add( inUse( new RelationshipRecord( 12, 103, 104, 0 ) ) );
        inconsistentRecord2.setSecondNextRel( 10 );
        RelationshipRecord consistentRecord = records.add( inUse( new RelationshipRecord( 10, 101, 104, 0 ) ) );
        consistentRecord.setFirstPrevRel( 11 );
        consistentRecord.setSecondPrevRel( 12 );

        NodeRecord nodeRecord = records.add( inUse( new NodeRecord( 101, 10, -1 ) ) );
        records.add( inUse( new NodeRecord( 102, 1002, -1 ) ) );
        records.add( inUse( new NodeRecord( 103, 1003, -1 ) ) );
        records.add( inUse( new NodeRecord( 104, 1004, -1 ) ) );

        RelationshipRecord unrelatedRelationship = records.add( inUse( new RelationshipRecord( 21, 201, 202, 0 ) ) );
        records.add( notInUse( new NodeRecord( 201, -1, -1 ) ) );
        records.add( notInUse( new NodeRecord( 202, -1, -1 ) ) );

        RecordRemover remover = mock( RecordRemover.class );
        Diagnostics diagnostics = mock( Diagnostics.class );

        RelationshipRemovalStrategy removalStrategy = new RelationshipRemovalStrategy( records, remover, diagnostics );

        // when
        removalStrategy.evaluate( asList( inconsistentRecord1, inconsistentRecord2, unrelatedRelationship ) );

        // then
        verify( diagnostics, atLeast( 1 ) ).removalOfRelationshipsPreventedBy( nodeRecord, RecordSet.asSet(consistentRecord, inconsistentRecord1, inconsistentRecord2) );

        verify( remover ).remove( unrelatedRelationship );
    }

    @Test
    public void shouldReportInabilityToRemoveReferencedRelationshipRecord() throws Exception
    {
        // given
        RecordAccessStub records = new RecordAccessStub();

        RelationshipRecord inconsistentRecord1 = records.add( inUse( new RelationshipRecord( 11, 101, 102, 0 ) ) );
        records.add( notInUse( new NodeRecord( 101, -1, -1 ) ) );
        NodeRecord nodeRecord = records.add( inUse( new NodeRecord( 102, 11, -1 ) ) );

        RecordRemover remover = mock( RecordRemover.class );
        Diagnostics diagnostics = mock( Diagnostics.class );

        RelationshipRemovalStrategy removalStrategy = new RelationshipRemovalStrategy( records, remover, diagnostics );

        // when
        removalStrategy.evaluate( asList( inconsistentRecord1 ) );

        // then
        verifyZeroInteractions( remover );
        verify( diagnostics, atLeast( 1 ) ).removalOfRelationshipsPreventedBy( nodeRecord,
                RecordSet.asSet( inconsistentRecord1 ) );
    }
}

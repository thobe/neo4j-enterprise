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
package org.neo4j.backup.consistency.checking;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Suite.class)
@Suite.SuiteClasses({DynamicRecordCheckTest.StringStore.class, DynamicRecordCheckTest.ArrayStore.class})
public abstract class DynamicRecordCheckTest
    extends RecordCheckTestBase<DynamicRecord,ConsistencyReport.DynamicConsistencyReport,DynamicRecordCheck>
{
    private final int blockSize;

    private DynamicRecordCheckTest( DynamicRecordCheck check, int blockSize )
    {
        super( check, ConsistencyReport.DynamicConsistencyReport.class );
        this.blockSize = blockSize;
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = notInUse( record( 42 ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( fill( record( 42 ), blockSize / 2 ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordWithConsistentReferences() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( fill( record( 42 ) ) );
        DynamicRecord next = add( inUse( fill( record( 7 ), blockSize / 2 ) ) );
        property.setNextBlock( next.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( fill( record( 42 ) ) );
        DynamicRecord next = add( notInUse( record( 7 ) ) );
        property.setNextBlock( next.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).nextNotInUse( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSelfReferentialNext() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = add( inUse( fill( record( 42 ) ) ) );
        property.setNextBlock( property.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).selfReferentialNext();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNonFullRecordWithNextReference() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( fill( record( 42 ), blockSize - 1 ) );
        DynamicRecord next = add( inUse( fill( record( 7 ), blockSize / 2 ) ) );
        property.setNextBlock( next.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).recordNotFullReferencesNext();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportInvalidDataLength() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( record( 42 ) );
        property.setLength( -1 );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).invalidLength();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyRecord() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( record( 42 ) );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).emptyBlock();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRecordWithEmptyNext() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        DynamicRecord property = inUse( fill( record( 42 ) ) );
        DynamicRecord next = add( inUse( record( 7 ) ) );
        property.setNextBlock( next.getId() );

        // when
        ConsistencyReport.DynamicConsistencyReport report = check( property );

        // then
        verify( report ).emptyNextBlock( next );
        verifyOnlyReferenceDispatch( report );
    }

    DynamicRecord fill( DynamicRecord record )
    {
        return fill( record, blockSize );
    }

    abstract DynamicRecord fill( DynamicRecord record, int size );

    abstract DynamicRecord record( long id );

    @RunWith(JUnit4.class)
    public static class StringStore extends DynamicRecordCheckTest
    {
        public StringStore()
        {
            super( new DynamicRecordCheck( configure( 66 ), DynamicRecordCheck.StoreDereference.STRING ), 66 );
        }

        @Override
        DynamicRecord record( long id )
        {
            return string( new DynamicRecord( id ) );
        }

        @Override
        DynamicRecord fill( DynamicRecord record, int size )
        {
            record.setLength( size );
            return record;
        }
    }

    @RunWith(JUnit4.class)
    public static class ArrayStore extends DynamicRecordCheckTest
    {
        public ArrayStore()
        {
            super( new DynamicRecordCheck( configure( 66 ), DynamicRecordCheck.StoreDereference.ARRAY ), 66 );
        }

        @Override
        DynamicRecord record( long id )
        {
            return array( new DynamicRecord( id ) );
        }

        @Override
        DynamicRecord fill( DynamicRecord record, int size )
        {
            record.setLength( size );
            return record;
        }
    }

    static RecordStore<DynamicRecord> configure( int blockSize )
    {
        @SuppressWarnings( "unchecked" )
        RecordStore<DynamicRecord> mock = mock( RecordStore.class );
        when( mock.getRecordSize() ).thenReturn( blockSize + AbstractDynamicStore.BLOCK_HEADER_SIZE );
        when( mock.getRecordHeaderSize() ).thenReturn( AbstractDynamicStore.BLOCK_HEADER_SIZE );
        return mock;
    }
}

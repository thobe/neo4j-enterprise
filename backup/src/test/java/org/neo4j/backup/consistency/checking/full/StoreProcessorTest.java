package org.neo4j.backup.consistency.checking.full;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

public class StoreProcessorTest
{
    @Test
    public void shouldProcessAllTheRecordsInAStore() throws Exception
    {
        // given
        StoreProcessor processor = new StoreProcessor( false, mock( ConsistencyReport.Reporter.class ) );
        RecordStore recordStore = mock( RecordStore.class );
        when( recordStore.getHighId() ).thenReturn( 3L );
        when( recordStore.forceGetRecord( any( Long.class ) ) ).thenReturn( new NodeRecord( 0, 0, 0 ) );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).forceGetRecord( 0 );
        verify( recordStore ).forceGetRecord( 1 );
        verify( recordStore ).forceGetRecord( 2 );
        verify( recordStore ).forceGetRecord( 3 );
    }

    @Test
    public void shouldStopProcessingRecordsWhenSignalledToStop() throws Exception
    {
        // given
        final StoreProcessor processor = new StoreProcessor( false, mock( ConsistencyReport.Reporter.class ) );
        RecordStore recordStore = mock( RecordStore.class );
        when( recordStore.getHighId() ).thenReturn( 3L );
        when( recordStore.forceGetRecord( 0L ) ).thenReturn( new NodeRecord( 0, 0, 0 ) );
        when( recordStore.forceGetRecord( 1L ) ).thenReturn( new NodeRecord( 0, 0, 0 ) );
        when( recordStore.forceGetRecord( 2L ) ).thenAnswer( new Answer<NodeRecord>()
        {
            @Override
            public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
            {
                processor.stopScanning();
                return new NodeRecord( 0, 0, 0 );
            }
        } );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).forceGetRecord( 0 );
        verify( recordStore ).forceGetRecord( 1 );
        verify( recordStore ).forceGetRecord( 2 );
        verify( recordStore, never() ).forceGetRecord( 3 );
    }
}

package org.neo4j.backup.consistency.checking;

import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

import static org.mockito.Mockito.verify;

public class NeoStoreCheckTest
        extends RecordCheckTestBase<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport, NeoStoreCheck>
{
    public NeoStoreCheckTest()
    {
        super( new NeoStoreCheck(), ConsistencyReport.NeoStoreConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordWithNoPropertyReference() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordWithConsistentReferenceToProperty() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        record.setNextProp( add( inUse( new PropertyRecord( 7 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        PropertyRecord property = add( notInUse( new PropertyRecord( 7 ) ) );
        record.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        NeoStoreRecord record = new NeoStoreRecord();
        PropertyRecord property = add( inUse( new PropertyRecord( 7 ) ) );
        property.setPrevProp( 6 );
        record.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = check( record );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    // Change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedRecord() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();

        oldRecord.setNextProp( addChange( inUse( new PropertyRecord( 1 ) ),
                                          notInUse( new PropertyRecord( 1 ) ) ).getId() );

        newRecord.setNextProp( addChange( notInUse( new PropertyRecord( 2 ) ),
                                          inUse( new PropertyRecord( 2 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportProblemsWithTheNewStateWhenCheckingChanges() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();

        oldRecord.setNextProp( addChange( inUse( new PropertyRecord( 1 ) ),
                                          notInUse( new PropertyRecord( 1 ) ) ).getId() );

        PropertyRecord property = addChange( notInUse( new PropertyRecord( 2 ) ),
                                             inUse( new PropertyRecord( 2 ) ) );
        property.setPrevProp( 10 );
        newRecord.setNextProp( property.getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyChainReplacedButNotUpdated() throws Exception
    {
        // given
        NeoStoreRecord oldRecord = new NeoStoreRecord();
        NeoStoreRecord newRecord = new NeoStoreRecord();
        oldRecord.setNextProp( add( inUse( new PropertyRecord( 1 ) ) ).getId() );
        newRecord.setNextProp( addChange( notInUse( new PropertyRecord( 2 ) ),
                                          inUse( new PropertyRecord( 2 ) ) ).getId() );

        // when
        ConsistencyReport.NeoStoreConsistencyReport report = checkChange( oldRecord, newRecord );

        // then
        verify( report ).propertyNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }
}

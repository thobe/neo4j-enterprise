package org.neo4j.backup.consistency.checking;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

public class PropertyRecordCheckTest
        extends RecordCheckTestBase<PropertyRecord, ConsistencyReport.PropertyConsistencyReport, PropertyRecordCheck>
{
    public PropertyRecordCheckTest()
    {
        super( new PropertyRecordCheck(), ConsistencyReport.PropertyConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForPropertyRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = notInUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForPropertyWithoutBlocksThatDoesNotReferenceAnyOtherRecords() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyKeyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( notInUse( new PropertyIndexRecord( 0 ) ) );
        PropertyBlock block = propertyBlock( key, PropertyType.INT, 0 );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).keyNotInUse( block, key );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).previousNotInUse( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextNotInUse( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotReferringBack() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( inUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).previousDoesNotReferenceBack( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotReferringBack() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( inUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextDoesNotReferenceBack( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportStringRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );
        // then
        verify( report ).stringNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportArrayRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyStringRecord() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( inUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).stringEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyArrayRecord() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( inUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Ignore
    @Test
    public void shouldNotReportAnythingForConsistentlyChangedProperty() throws Exception
    {
        // given

        // when

        // then
        fail( "needs to be implemented" );
    }

    @Ignore
    @Test
    public void shouldReportPreviousReplacedButNotUpdated() throws Exception
    {
        // given

        // when

        // then
        fail( "needs to be implemented" );
    }

    @Ignore
    @Test
    public void shouldReportNextReplacedButNotUpdated() throws Exception
    {
        // given

        // when

        // then
        fail( "needs to be implemented" );
    }

    @Ignore
    @Test
    public void shouldReportStringValueUnreferencedButNotUpdated() throws Exception
    {
        // given

        // when

        // then
        fail( "needs to be implemented" );
    }

    @Ignore
    @Test
    public void shouldReportArrayValueUnreferencedButNotUpdated() throws Exception
    {
        // given

        // when

        // then
        fail( "needs to be implemented" );
    }
}

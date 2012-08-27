package org.neo4j.backup.consistency.check;

import org.junit.Test;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

import static org.mockito.Mockito.mock;
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
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = notInUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForPropertyWithoutBlocksThatDoesNotReferenceAnyOtherRecords() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyKeyNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( records, notInUse( new PropertyIndexRecord( 0 ) ) );
        PropertyBlock block = propertyBlock( key, PropertyType.INT, 0 );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).keyNotInUse( block, key );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( records, notInUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).previousNotInUse( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( records, notInUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).nextNotInUse( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotReferringBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( records, inUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).previousDoesNotReferenceBack( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotReferringBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( records, inUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).nextDoesNotReferenceBack( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportStringRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( records, inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( records, notInUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );
        // then
        verify( report ).stringNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportArrayRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( records, inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( records, notInUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).arrayNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyStringRecord() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( records, inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( records, inUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).stringEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyArrayRecord() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( records, inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( records, inUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property, records );

        // then
        verify( report ).arrayEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }
}

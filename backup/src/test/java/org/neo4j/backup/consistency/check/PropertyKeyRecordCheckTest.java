package org.neo4j.backup.consistency.check;

import org.junit.Test;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PropertyKeyRecordCheckTest extends
        RecordCheckTestBase<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport, PropertyKeyRecordCheck>
{
    public PropertyKeyRecordCheckTest()
    {
        super( new PropertyKeyRecordCheck(), ConsistencyReport.PropertyKeyConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyIndexRecord key = notInUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addName( records, notInUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key, records );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addName( records, inUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key, records );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }

    private DynamicRecord addName( RecordAccess records, DynamicRecord name )
    {
        when( records.getKeyName( (int) name.getId() ) ).thenReturn( name );
        return name;
    }
}

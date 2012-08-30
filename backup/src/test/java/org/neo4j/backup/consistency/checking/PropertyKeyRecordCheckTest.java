package org.neo4j.backup.consistency.checking;

import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;

import static org.mockito.Mockito.verify;

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
        PropertyIndexRecord key = notInUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( notInUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        PropertyIndexRecord key = inUse( new PropertyIndexRecord( 42 ) );
        DynamicRecord name = addKeyName( inUse( new DynamicRecord( 6 ) ) );
        key.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.PropertyKeyConsistencyReport report = check( key );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }
}

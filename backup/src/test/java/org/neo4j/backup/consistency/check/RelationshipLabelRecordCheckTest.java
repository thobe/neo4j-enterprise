package org.neo4j.backup.consistency.check;

import org.junit.Test;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RelationshipLabelRecordCheckTest extends
        RecordCheckTestBase<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport, RelationshipLabelRecordCheck>
{
    public RelationshipLabelRecordCheckTest()
    {
        super( new RelationshipLabelRecordCheck(), ConsistencyReport.LabelConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRecordNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipTypeRecord label = notInUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addName( records, notInUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label, records );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addName( records, inUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label, records );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }

    private DynamicRecord addName( RecordAccess records, DynamicRecord name )
    {
        when( records.getLabelName( (int) name.getId() ) ).thenReturn( name );
        return name;
    }
}

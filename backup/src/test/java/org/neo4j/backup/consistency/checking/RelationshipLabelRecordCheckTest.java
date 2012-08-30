package org.neo4j.backup.consistency.checking;

import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static org.mockito.Mockito.verify;

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
        RelationshipTypeRecord label = notInUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRecordThatDoesNotReferenceADynamicBlock() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportDynamicBlockNotInUse() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( notInUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verify( report ).nameBlockNotInUse( name );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyName() throws Exception
    {
        // given
        RelationshipTypeRecord label = inUse( new RelationshipTypeRecord( 42 ) );
        DynamicRecord name = addLabelName( inUse( new DynamicRecord( 6 ) ) );
        label.setNameId( (int) name.getId() );

        // when
        ConsistencyReport.LabelConsistencyReport report = check( label );

        // then
        verify( report ).emptyName( name );
        verifyOnlyReferenceDispatch( report );
    }
}

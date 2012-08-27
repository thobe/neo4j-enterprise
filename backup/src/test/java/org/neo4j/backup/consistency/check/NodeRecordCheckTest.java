package org.neo4j.backup.consistency.check;

import org.junit.Test;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NodeRecordCheckTest
        extends RecordCheckTestBase<NodeRecord, ConsistencyReport.NodeConsistencyReport, NodeRecordCheck>
{
    public NodeRecordCheckTest()
    {
        super( new NodeRecordCheck(), ConsistencyReport.NodeConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForNodeNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = notInUse( new NodeRecord( 42, 0, 0 ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, NONE, NONE ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentReferences() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        add( records, inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        add( records, inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        RelationshipRecord relationship = add( records, notInUse( new RelationshipRecord( 7, 0, 0, 0 ) ) );
        add( records, inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).relationshipNotInUse( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( records, notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( records, inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipForOtherNodes() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 7, 1, 2, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).relationshipForOtherNode( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInSourceChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInTargetChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 7, 0, 42, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportLoopRelationshipNotFirstInTargetAndSourceChains() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( records, inUse( new RelationshipRecord( 7, 42, 42, 0 ) ) );
        relationship.setFirstPrevRel( 8 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node, records );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }
}

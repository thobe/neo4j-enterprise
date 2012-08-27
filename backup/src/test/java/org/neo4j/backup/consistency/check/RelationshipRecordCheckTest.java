package org.neo4j.backup.consistency.check;

import org.junit.Test;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RelationshipRecordCheckTest extends
        RecordCheckTestBase<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport, RelationshipRecordCheck>
{
    public RelationshipRecordCheckTest()
    {
        super( new RelationshipRecordCheck(), ConsistencyReport.RelationshipConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = notInUse( new RelationshipRecord( 42, 0, 0, 0 ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForRelationshipWithConsistentReferences() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, relationship.getId(), NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 53, NONE ) ) );
        add( records, inUse( new NodeRecord( 3, NONE, NONE ) ) );
        add( records, inUse( new PropertyRecord( 101 ) ) );
        relationship.setNextProp( 101 );
        RelationshipRecord sNext = add( records, inUse( new RelationshipRecord( 51, 1, 3, 4 ) ) );
        RelationshipRecord tNext = add( records, inUse( new RelationshipRecord( 52, 2, 3, 4 ) ) );
        RelationshipRecord tPrev = add( records, inUse( new RelationshipRecord( 53, 3, 2, 4 ) ) );

        relationship.setFirstNextRel( sNext.getId() );
        sNext.setFirstPrevRel( relationship.getId() );
        relationship.setSecondNextRel( tNext.getId() );
        tNext.setFirstPrevRel( relationship.getId() );
        relationship.setSecondPrevRel( tPrev.getId() );
        tPrev.setSecondNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportIllegalLabel() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, NONE ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).illegalLabel();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportLabelNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        RelationshipTypeRecord label = add( records, notInUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).labelNotInUse( label );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportIllegalSourceNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, NONE, 1, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).illegalSourceNode();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNodeNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        NodeRecord node = add( records, notInUse( new NodeRecord( 1, NONE, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNodeNotInUse( node );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportIllegalTargetNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, NONE, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).illegalTargetNode();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNodeNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        NodeRecord node = add( records, notInUse( new NodeRecord( 2, NONE, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNodeNotInUse( node );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        PropertyRecord property = add( records, notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        relationship.setNextProp( 11 );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        PropertyRecord property = add( records, inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNodeNotReferencingBackForFirstRelationshipInSourceChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        NodeRecord source = add( records, inUse( new NodeRecord( 1, 7, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNodeNotReferencingBackForFirstRelationshipInTargetChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        NodeRecord target = add( records, inUse( new NodeRecord( 2, 7, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceAndTargetNodeNotReferencingBackForFirstRelationshipInChains() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        NodeRecord source = add( records, inUse( new NodeRecord( 1, NONE, NONE ) ) );
        NodeRecord target = add( records, inUse( new NodeRecord( 2, NONE, NONE ) ) );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNodeDoesNotReferenceBack( source );
        verify( report ).targetNodeDoesNotReferenceBack( target );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNodeWithoutChainForRelationshipInTheMiddleOfChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        NodeRecord source = add( records, inUse( new NodeRecord( 1, NONE, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sPrev = add( records, inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );
        sPrev.setFirstNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNodeHasNoRelationships( source );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNodeWithoutChainForRelationshipInTheMiddleOfChain() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        NodeRecord target = add( records, inUse( new NodeRecord( 2, NONE, NONE ) ) );
        RelationshipRecord tPrev = add( records, inUse( new RelationshipRecord( 51, 0, 2, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );
        tPrev.setSecondNextRel( relationship.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNodeHasNoRelationships( target );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodes() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 0, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sPrev = add( records, inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodes() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 0, NONE ) ) );
        RelationshipRecord tPrev = add( records, inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodes() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sNext = add( records, inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodes() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord tNext = add( records, inUse( new RelationshipRecord( 51, 8, 9, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourcePrevReferencingOtherNodesWhenReferencingTargetNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 0, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sPrev = add( records, inUse( new RelationshipRecord( 51, 2, 0, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourcePrevReferencesOtherNodes( sPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetPrevReferencingOtherNodesWhenReferencingSourceNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 0, NONE ) ) );
        RelationshipRecord tPrev = add( records, inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetPrevReferencesOtherNodes( tPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNextReferencingOtherNodesWhenReferencingTargetNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sNext = add( records, inUse( new RelationshipRecord( 51, 2, 0, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNextReferencesOtherNodes( sNext );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNextReferencingOtherNodesWhenReferencingSourceNode() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord tNext = add( records, inUse( new RelationshipRecord( 51, 1, 0, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNextReferencesOtherNodes( tNext );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourcePrevNotReferencingBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 0, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sPrev = add( records, inUse( new RelationshipRecord( 51, 1, 3, 0 ) ) );
        relationship.setFirstPrevRel( sPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourcePrevDoesNotReferenceBack( sPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetPrevNotReferencingBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 0, NONE ) ) );
        RelationshipRecord tPrev = add( records, inUse( new RelationshipRecord( 51, 2, 3, 0 ) ) );
        relationship.setSecondPrevRel( tPrev.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetPrevDoesNotReferenceBack( tPrev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportSourceNextNotReferencingBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord sNext = add( records, inUse( new RelationshipRecord( 51, 3, 1, 0 ) ) );
        relationship.setFirstNextRel( sNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).sourceNextDoesNotReferenceBack( sNext );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportTargetNextNotReferencingBack() throws Exception
    {
        // given
        RecordAccess records = mock( RecordAccess.class );
        RelationshipRecord relationship = inUse( new RelationshipRecord( 42, 1, 2, 4 ) );
        add( records, inUse( new RelationshipTypeRecord( 4 ) ) );
        add( records, inUse( new NodeRecord( 1, 42, NONE ) ) );
        add( records, inUse( new NodeRecord( 2, 42, NONE ) ) );
        RelationshipRecord tNext = add( records, inUse( new RelationshipRecord( 51, 3, 2, 0 ) ) );
        relationship.setSecondNextRel( tNext.getId() );

        // when
        ConsistencyReport.RelationshipConsistencyReport report = check( relationship, records );

        // then
        verify( report ).targetNextDoesNotReferenceBack( tNext );
        verifyOnlyReferenceDispatch( report );
    }
}

package org.neo4j.backup.consistency.checking;

import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

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
        NodeRecord node = notInUse( new NodeRecord( 42, 0, 0 ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeThatDoesNotReferenceOtherRecords() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, NONE ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForNodeWithConsistentReferences() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, 11 ) );
        RelationshipRecord relationship = add( notInUse( new RelationshipRecord( 7, 0, 0, 0 ) ) );
        add( inUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotInUse( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotInUse() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( notInUse( new PropertyRecord( 11 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotInUse( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyNotFirstInChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, NONE, 11 ) );
        PropertyRecord property = add( inUse( new PropertyRecord( 11 ) ) );
        property.setPrevProp( 6 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).propertyNotFirstInChain( property );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipForOtherNodes() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 1, 2, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipForOtherNode( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInSourceChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 0, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipNotFirstInTargetChain() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 0, 42, 0 ) ) );
        relationship.setFirstPrevRel( 6 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportLoopRelationshipNotFirstInTargetAndSourceChains() throws Exception
    {
        // given
        NodeRecord node = inUse( new NodeRecord( 42, 7, NONE ) );
        RelationshipRecord relationship = add( inUse( new RelationshipRecord( 7, 42, 42, 0 ) ) );
        relationship.setFirstPrevRel( 8 );
        relationship.setSecondPrevRel( 8 );

        // when
        ConsistencyReport.NodeConsistencyReport report = check( node );

        // then
        verify( report ).relationshipNotFirstInSourceChain( relationship );
        verify( report ).relationshipNotFirstInTargetChain( relationship );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedNode() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 11, 1 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 12, 2 ) );

        addChange( inUse( new RelationshipRecord( 11, 42, 0, 0 ) ),
                   notInUse( new RelationshipRecord( 11, 0, 0, 0 ) ) );
        addChange( notInUse( new RelationshipRecord( 12, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 12, 42, 0, 0 ) ) );

        addChange( inUse( new PropertyRecord( 1 ) ),
                   notInUse( new PropertyRecord( 1 ) ) );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyChainReplacedButNotUpdated() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, NONE, 1 ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, NONE, 2 ) );
        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).propertyReplacedButNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportRelationshipChainReplacedButNotUpdated() throws Exception
    {
        // given
        NodeRecord oldNode = inUse( new NodeRecord( 42, 1, NONE ) );
        NodeRecord newNode = inUse( new NodeRecord( 42, 2, NONE ) );
        addChange( notInUse( new RelationshipRecord( 2, 0, 0, 0 ) ),
                   inUse( new RelationshipRecord( 2, 42, 0, 0 ) ) );

        // when
        ConsistencyReport.NodeConsistencyReport report = checkChange( oldNode, newNode );

        // then
        verify( report ).relationshipReplacedButNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }
}

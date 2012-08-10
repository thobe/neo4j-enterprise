package org.neo4j.backup.check;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.check.RecordSet.asSet;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_PROPERTY;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class OwningNodeRelationshipChainTest
{
    @Test
    public void shouldFindBothChainsThatTheRelationshipRecordShouldBelongTo() throws Exception
    {
        // given
        int node1 = 101, node1Rel = 1001;
        int node2 = 201, node2Rel = 2001;
        int sharedRel = 1000;
        int relType = 0;

        RecordSet<RelationshipRecord> node1RelChain = asSet(
                new RelationshipRecord( node1Rel, node1, node1 - 1, relType ),
                new RelationshipRecord( sharedRel, node1, node2, relType ),
                new RelationshipRecord( node1Rel + 1, node1 + 1, node1, relType ) );
        RecordSet<RelationshipRecord> node2RelChain = asSet(
                new RelationshipRecord( node2Rel, node2 - 1, node2, relType ),
                new RelationshipRecord( sharedRel, node1, node2, relType ),
                new RelationshipRecord( node2Rel + 1, node2, node2 + 1, relType ) );

        @SuppressWarnings("unchecked")
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.forceGetRecord( node1 ) ).thenReturn(
                new NodeRecord( node1, node1Rel, NO_NEXT_PROPERTY.intValue() ) );
        when( recordStore.forceGetRecord( node2 ) ).thenReturn(
                new NodeRecord( node2, node2Rel, NO_NEXT_PROPERTY.intValue() ) );

        RelationshipChainExplorer relationshipChainExplorer = mock( RelationshipChainExplorer.class );
        when( relationshipChainExplorer.followChainFromNode( node1, node1Rel ) ).thenReturn( node1RelChain );
        when( relationshipChainExplorer.followChainFromNode( node2, node2Rel ) ).thenReturn( node2RelChain );

        OwningNodeRelationshipChain owningChainFinder =
                new OwningNodeRelationshipChain( relationshipChainExplorer, recordStore );

        // when
        RecordSet<RelationshipRecord> recordsInChains = owningChainFinder
                .findRelationshipChainsThatThisRecordShouldBelongTo( new RelationshipRecord( sharedRel, node1, node2,
                        relType ) );

        // then
        assertThat( recordsInChains, containsAllRecords( node1RelChain ) );
        assertThat( recordsInChains, containsAllRecords( node2RelChain ) );
    }

    private Matcher<RecordSet<RelationshipRecord>> containsAllRecords( final RecordSet<RelationshipRecord> expectedSet )
    {
        return new TypeSafeMatcher<RecordSet<RelationshipRecord>>()
        {
            @Override
            public boolean matchesSafely( RecordSet<RelationshipRecord> actualSet )
            {
                return actualSet.containsAll( expectedSet );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "RecordSet containing " ).appendValueList( "[", ",", "]", expectedSet );
            }
        };
    }


}

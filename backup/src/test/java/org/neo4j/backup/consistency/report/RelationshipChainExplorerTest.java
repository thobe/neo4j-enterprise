package org.neo4j.backup.consistency.report;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.test.TargetDirectory;

public class RelationshipChainExplorerTest
{
    @Test
    public void shouldLoadAllConnectedRelationshipRecordsAndTheirFullChainsOfRelationshipRecords() throws Exception
    {
        // given
        int nDegreeTwoNodes = 10;
        StoreAccess store = createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes( nDegreeTwoNodes );
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain ) );

        // then
        assertEquals( nDegreeTwoNodes * 2, records.size() );
    }

    @Test
    public void shouldCopeWithAChainThatReferencesNotInUseZeroValueRecords() throws Exception
    {
        // given
        int nDegreeTwoNodes = 10;
        StoreAccess store = createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes( nDegreeTwoNodes );
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();
        breakTheChain( relationshipStore );

        // when
        int relationshipIdInMiddleOfChain = 10;
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer( relationshipStore )
                .exploreRelationshipRecordChainsToDepthTwo(
                        relationshipStore.getRecord( relationshipIdInMiddleOfChain ) );

        // then
        int recordsInaccessibleBecauseOfBrokenChain = 3;
        assertEquals( nDegreeTwoNodes * 2 - recordsInaccessibleBecauseOfBrokenChain, records.size() );
    }

    private void breakTheChain( RecordStore<RelationshipRecord> relationshipStore )
    {
        int relationshipTowardsEndOfChain = 16;
        relationshipStore.updateRecord( new RelationshipRecord( relationshipTowardsEndOfChain, 0, 0, 0 ) );
    }

    enum TestRelationshipType implements RelationshipType
    {
        CONNECTED
    }

    private StoreAccess createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes( int nDegreeTwoNodes )
    {
        File storeDirectory = TargetDirectory.forTest( getClass() ).graphDbDir( true );
        EmbeddedGraphDatabase database = new EmbeddedGraphDatabase( storeDirectory.getPath() );
        Transaction transaction = database.beginTx();
        try
        {
            Node denseNode = database.createNode();
            for ( int i = 0; i < nDegreeTwoNodes; i++ )
            {
                Node degreeTwoNode = database.createNode();
                Node leafNode = database.createNode();
                if ( i % 2 == 0 )
                {
                    denseNode.createRelationshipTo( degreeTwoNode, TestRelationshipType.CONNECTED );
                }
                else
                {
                    degreeTwoNode.createRelationshipTo( denseNode, TestRelationshipType.CONNECTED );
                }
                degreeTwoNode.createRelationshipTo( leafNode, TestRelationshipType.CONNECTED );
            }
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
        database.shutdown();
        return new StoreAccess( storeDirectory.getPath() );
    }
}

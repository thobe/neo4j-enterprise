package org.neo4j.backup.check;

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
        StoreAccess store = createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes();
        RecordStore<RelationshipRecord> relationshipStore = store.getRelationshipStore();

        // when
        RecordSet<RelationshipRecord> records = new RelationshipChainExplorer(relationshipStore)
                .findInterestingRecordsAroundInconsistency( relationshipStore.getRecord( 10 ) );

        // then
        assertEquals(20, records.size());
    }

    enum TestRelationshipType implements RelationshipType {
        CONNECTED
    }

    private StoreAccess createStoreWithOneHighDegreeNodeAndSeveralDegreeTwoNodes()
    {
        File storeDirectory = TargetDirectory.forTest( getClass() ).graphDbDir( true );
        EmbeddedGraphDatabase database = new EmbeddedGraphDatabase( storeDirectory.getPath() );
        Transaction transaction = database.beginTx();
        try {
            Node denseNode = database.createNode();
            for (int i = 0; i < 10; i++)
            {
                Node degreeTwoNode = database.createNode();
                Node leafNode = database.createNode();
                denseNode.createRelationshipTo( degreeTwoNode, TestRelationshipType.CONNECTED );
                degreeTwoNode.createRelationshipTo( leafNode, TestRelationshipType.CONNECTED );
            }
            transaction.success();
        } finally {
            transaction.finish();
        }
        database.shutdown();
        return new StoreAccess( storeDirectory.getPath() );
    }
}

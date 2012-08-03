package org.neo4j.ha;

import static org.neo4j.kernel.configuration.Config.DEFAULT_DATA_SOURCE_NAME;

import java.util.Queue;

import org.neo4j.com.RequestContext;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class FakeMaster implements Master
{
    public static final long ANY_CHECKSUM = 0L;
    public static final int MASTER_ID = 1;

    private final Queue<Triplet<String, Long, TxExtractor>> dataQueue;
    private final TransactionStream transactionStream = new TransactionStream( DEFAULT_DATA_SOURCE_NAME )
    {
        @Override
        protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
        {
            return dataQueue.poll();
        }
    };

    public FakeMaster( Queue<Triplet<String, Long, TxExtractor>> dataQueue )
    {
        this.dataQueue = dataQueue;
    }

    @Override
    public Response<IdAllocation> allocateIds( IdType idType )
    {
        return null;
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        return null;
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context, long... relationships )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( RequestContext context, long... relationships )
    {
        return null;
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context, String resource, TxExtractor
            txGetter )
    {
        return null;
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        return null;
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return new Response<Void>( null, new StoreId(), transactionStream, ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId myStoreId )
    {
        return new Response<Pair<Integer, Long>>( Pair.of( MASTER_ID, ANY_CHECKSUM ), myStoreId,
                TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Void> copyStore( RequestContext context, StoreWriter writer )
    {
        return null;
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context, String dsName, long startTxId, long endTxId )
    {
        return null;
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key )
    {
        return null;
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return null;
    }

    @Override
    public Response<Void> pushTransaction( RequestContext context, String resourceName, long tx )
    {
        return null;
    }

}

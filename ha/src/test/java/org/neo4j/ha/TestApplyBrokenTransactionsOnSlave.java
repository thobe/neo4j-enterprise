package org.neo4j.ha;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.DEFAULT_DATA_SOURCE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.RequestContext;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.CommandAccessFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.test.TargetDirectory;

public class TestApplyBrokenTransactionsOnSlave
{
    public static final long ANY_CHECKSUM = 0L;
    public static final int MASTER_ID = 1;
    public static final long TX_ID = 1;

    private final File dir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private ReadableByteChannel data;

    {
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        try
        {
            LogIoUtils.writeStart( buffer, 0, new XidImpl( XidImpl.getNewGlobalId(), NeoStoreXaDataSource.BRANCH_ID ), 0, 0, 0l );
            LogIoUtils.writeCommand( buffer, 0, CommandAccessFactory.relationship( 0l, 0l, 0l, 0, 0l, 0l, 0l, 0l ) );
            LogIoUtils.writeCommit( false, buffer, 0, TX_ID, System.currentTimeMillis() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        data = buffer;
    }

    @Test
    public void shouldTriggerRecoveryAfterApplyingATransactionFailsDueToRuntimeException() throws Exception
    {
        // use a non-ha database to create some store files, because without them an HA database won't start
        new EmbeddedGraphDatabase( dir.getAbsolutePath() ).shutdown();

        final Config config = new Config( new ConfigurationDefaults( GraphDatabaseSettings.class,
                HaSettings.class ).apply( new HashMap<String, String>() ) );

        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( dir.getAbsolutePath(), stringMap( HaSettings.server_id.name(), "1" ) )
        {
            @Override
            protected Broker createBroker()
            {
                return new FakeSlaveBroker( new FakeMaster(), 1, config );
            }

            @Override
            protected ClusterClient createClusterClient()
            {
                return new FakeClusterClient( getBroker() );
            }
        };

        db.pullUpdates();
    }

    private void startADatabaseThatAlwaysThinksTheFakeServerIsMaster()
    {
        // use a non-ha database to create some store files, because without them an HA database won't start
        new EmbeddedGraphDatabase( dir.getAbsolutePath() ).shutdown();

        final Config config = new Config( new ConfigurationDefaults( GraphDatabaseSettings.class,
                HaSettings.class ).apply( new HashMap<String, String>() ) );

        HighlyAvailableGraphDatabase db = new HighlyAvailableGraphDatabase( dir.getAbsolutePath(), stringMap( HaSettings.server_id.name(), "1" ) )
        {
            @Override
            protected Broker createBroker()
            {
                return new FakeSlaveBroker( new FakeMaster(), 1, config );
            }

            @Override
            protected ClusterClient createClusterClient()
            {
                return new FakeClusterClient( getBroker() );
            }
        };
    }


    @Test
    public void shouldStartUpANonHaDatabase() throws Exception
    {
        new EmbeddedGraphDatabase( dir.getAbsolutePath() ).shutdown();
    }

    private class FakeMaster implements Master
    {
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
            return new Response<Void>( null, new StoreId(), new TransactionStreamContainingBrokenTransactions(), ResourceReleaser.NO_OP );
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

    private class TransactionStreamContainingBrokenTransactions extends TransactionStream
    {
        Triplet<String, Long, TxExtractor> theOnlyTransaction = Triplet.of( DEFAULT_DATA_SOURCE_NAME, TX_ID,
                TxExtractor.create( data ) );

        @Override
        protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
        {
            try
            {
                return theOnlyTransaction;
            }
            finally
            {
                theOnlyTransaction = null;
            }
        }
    }

}

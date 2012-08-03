package org.neo4j.ha;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.DEFAULT_DATA_SOURCE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.FakeClusterClient;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.nioneo.xa.CommandAccessFactory;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.test.TargetDirectory;

public class FakeMasterFixture implements TestRule
{
    public static final DataInitializer EMPTY_INITIAL_DATABASE = new DataInitializer()
    {
        @Override
        public void initialize( GraphDatabaseService db )
        {
            // do nothing
        }
    };

    public HighlyAvailableGraphDatabase startDatabase( DataInitializer seed )
    {
        if ( database != null )
        {
            throw new IllegalStateException( "already started" );
        }

        // use a non-ha database to create some store files, because without them an HA database won't start
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( dir.getAbsolutePath() );

        seed.initialize( db );

        db.shutdown();

        return slaveConnectedToFakeMaster();
    }

    public interface DataInitializer
    {
        void initialize( GraphDatabaseService db );
    }

    public void transaction( MasterTransaction tx )
    {
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        int localId = this.localIdGenerator++;
        long txId = this.txIdGenerator++;
        try
        {
            LogIoUtils.writeStart( buffer, localId, new XidImpl( XidImpl.getNewGlobalId(),
                    NeoStoreXaDataSource.BRANCH_ID ), FakeMaster.MASTER_ID, FakeMaster.MASTER_ID, System.currentTimeMillis() );

            tx.transactionData( new TransactionCommandFactory( buffer, localId ) );

            LogIoUtils.writeCommit( false, buffer, localId, txId, System.currentTimeMillis() );
            LogIoUtils.writeDone( buffer, localId );
        }
        catch ( IOException e )
        {
            throw new Error( "InMemoryLogBuffer should not throw exception", e );
        }
        txQueue.add( Triplet.of( DEFAULT_DATA_SOURCE_NAME, txId, TxExtractor.create( buffer ) ) );
    }

    public interface MasterTransaction
    {
        void transactionData( TransactionCommandFactory tx );
    }

    public static class TransactionCommandFactory
    {
        private final LogBuffer buffer;
        private final int localId;

        private TransactionCommandFactory( LogBuffer buffer, int localId )
        {
            this.buffer = buffer;
            this.localId = localId;
        }

        public void add( NodeRecord node )
        {
            write( CommandAccessFactory.command( node ) );
        }

        public void add( RelationshipRecord relationship )
        {
            write( CommandAccessFactory.command( relationship ) );
        }

        public void add( PropertyRecord property )
        {
            write( CommandAccessFactory.command( property ) );
        }

        private void write( Command command )
        {
            try
            {
                LogIoUtils.writeCommand( buffer, localId, command );
            }
            catch ( IOException e )
            {
                throw new Error( "InMemoryLogBuffer should not throw exception", e );
            }
        }
    }

    private HighlyAvailableGraphDatabase slaveConnectedToFakeMaster()
    {
        Map<String, String> settings = stringMap( HaSettings.server_id.name(), "1" );

        return this.database = new HighlyAvailableGraphDatabase( dir.getAbsolutePath(), settings )
        {
            @Override
            protected Broker createBroker()
            {
                return new FakeSlaveBroker( new FakeMaster( txQueue ), 1, this.configuration );
            }

            @Override
            protected ClusterClient createClusterClient()
            {
                return new FakeClusterClient( getBroker() );
            }
        };
    }

    private File dir;
    private HighlyAvailableGraphDatabase database;
    private final Queue<Triplet<String, Long, TxExtractor>> txQueue = new LinkedList<Triplet<String, Long, TxExtractor>>();
    private int localIdGenerator = 0;
    private int txIdGenerator = 2; // TODO: read this from store after db is initialized

    @Override
    public Statement apply( final Statement base, Description description )
    {
        final TargetDirectory.TestDirectory dir = TargetDirectory.forTest( description.getTestClass() ).testDirectory();
        return dir.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                FakeMasterFixture.this.dir = dir.directory();
                try
                {
                    base.evaluate();
                }
                finally
                {
                    FakeMasterFixture.this.cleanup();
                }
            }
        }, description );
    }

    private void cleanup()
    {
        if ( database != null )
        {
            database.shutdown();
        }
        this.database = null;
        this.dir = null;
    }
}

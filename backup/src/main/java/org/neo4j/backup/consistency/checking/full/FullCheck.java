package org.neo4j.backup.consistency.checking.full;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultLastCommittedTxIdSetter;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullCheck
{
    /** Defaults to false due to the way Boolean.parseBoolean(null) works. */
    public static final GraphDatabaseSetting<Boolean> consistency_check_property_owners =
            new GraphDatabaseSetting.BooleanSetting( "consistency_check_property_owners" );

    private final boolean checkPropertyOwners;
    private final Progress.Factory progressFactory;

    public FullCheck( boolean checkPropertyOwners, Progress.Factory progressFactory )
    {
        this.checkPropertyOwners = checkPropertyOwners;
        this.progressFactory = progressFactory;
    }

    public void execute( StoreAccess store, StringLogger logger )
    {
        execute( store, ConsistencyReporter.create( new SimpleRecordAccess( store ),
                                                    new DirectReferenceDispatcher(),
                                                    logger ) );
    }

    public void execute( StoreAccess store, ConsistencyReport.Reporter reporter )
    {
        StoreProcessor processor = new StoreProcessor( checkPropertyOwners, reporter );

        Progress.MultiPartBuilder progress = progressFactory.multipleParts( "Full consistency check" );
        List<Runnable> tasks = new ArrayList<Runnable>( 9 );

        tasks.add( new StoreProcessorTask<NodeRecord>( store.getNodeStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<RelationshipRecord>( store.getRelationshipStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<PropertyRecord>( store.getPropertyStore(), processor, progress ) );

        tasks.add( new StoreProcessorTask<RelationshipTypeRecord>( store.getRelationshipTypeStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<PropertyIndexRecord>( store.getPropertyIndexStore(), processor, progress ) );

        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getStringStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getArrayStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getTypeNameStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getPropertyKeyStore(), processor, progress ) );

        progress.complete();

        execute( tasks );

        processor.checkOrphanPropertyChains( progressFactory );
    }

    protected void execute( List<Runnable> tasks )
    {
        for ( Runnable task : tasks )
        {
            task.run();
        }
    }

    public static void run( Progress.Factory progressFactory, String storeDir, Config config, StringLogger logger )
    {
        StoreFactory factory = new StoreFactory( config,
                                                 new DefaultIdGeneratorFactory(),
                                                 new DefaultFileSystemAbstraction(),
                                                 new DefaultLastCommittedTxIdSetter(),
                                                 logger,
                                                 new DefaultTxHook() );
        NeoStore neoStore = factory.newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsolutePath() );
        try
        {
            StoreAccess store = new StoreAccess( neoStore );
            new FullCheck( config.get( consistency_check_property_owners ), progressFactory ).execute( store, logger );
        }
        finally
        {
            neoStore.close();
        }
    }

    private static class StoreProcessorTask<R extends AbstractBaseRecord> implements Runnable
    {
        private final RecordStore<R> store;
        private final StoreProcessor processor;
        private final Progress progress;

        StoreProcessorTask( RecordStore<R> store, StoreProcessor processor, Progress.MultiPartBuilder builder )
        {
            this.store = store;
            this.processor = processor;
            String name = store.getStorageFileName();
            this.progress = builder.progressForPart( name.substring( name.lastIndexOf( '/' ) + 1 ), store.getHighId() );
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run()
        {
            processor.applyFiltered( store, progress );
        }
    }
}

package org.neo4j.backup.consistency;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.backup.consistency.check.StoreProcessor;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.util.StringLogger;

class FullCheck
{
    private final boolean checkPropertyOwners;
    private final Progress.Factory progressFactory;
    private final StringLogger logger;

    public FullCheck( boolean checkPropertyOwners, Progress.Factory progressFactory, StringLogger logger )
    {
        this.checkPropertyOwners = checkPropertyOwners;
        this.progressFactory = progressFactory;
        this.logger = logger;
    }

    public void execute( StoreAccess store )
    {
        StoreProcessor processor = new StoreProcessor( checkPropertyOwners, ConsistencyReporter
                .create( new SimpleRecordAccess( store ), new DirectReferenceDispatcher(), logger ) );

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
    }

    protected void execute( List<Runnable> tasks )
    {
        for ( Runnable task : tasks )
        {
            task.run();
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

    public static void main( String... args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        args = params.orphans().toArray( new String[0] );
        if ( args.length != 1 )
        {
            printUsage();
            System.exit( -1 );
            return;
        }
        String storeDir = args[0];
        if ( !new File( storeDir ).isDirectory() )
        {
            printUsage( String.format( "'%s' is not a directory", storeDir ) );
            System.exit( -1 );
        }
        if ( params.getBoolean( "recovery", false, true ) )
        {
            new EmbeddedGraphDatabase( storeDir ).shutdown();
        }
        else
        {
            XaLogicalLogFiles logFiles = new XaLogicalLogFiles(
                    new File( storeDir, NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME ).getAbsolutePath(),
                    new DefaultFileSystemAbstraction() );
            try
            {
                switch ( logFiles.determineState() )
                {
                case LEGACY_WITHOUT_LOG_ROTATION:
                    printUsage( "WARNING: store contains log file from too old version." );
                    break;
                case NO_ACTIVE_FILE:
                case CLEAN:
                    break;
                default:
                    printUsage( "Active logical log detected, this might be a source of inconsistencies.",
                                "Consider allowing the database to recover before running the consistency check.",
                                "Consistency checking will continue, abort if you wish to perform recovery first.",
                                "To perform recovery before checking consistency, use the '--recovery' flag." );
                }
            }
            catch ( IOException e )
            {
                System.err.printf( "Failure when checking for active logs: '%s', continuing as normal.%n", e );
            }
        }
        StringLogger logger = StringLogger.SYSTEM;
        FullCheck check = new FullCheck( params.getBoolean( "propowner", false, true ),
                                         Progress.textual( System.err ), logger );
        try
        {
            check.execute( new StoreAccess( storeDir ) );
        }
        finally
        {
            logger.close();
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines )
        {
            System.err.println( line );
        }
        System.err.println( Args.jarUsage( FullCheck.class, "[-propowner] [-recovery] <storedir>" ) );
        System.err.println( "WHERE:   <storedir>  is the path to the store to check" );
        System.err.println( "         -propowner  --  to verify that properties are owned only once" );
        System.err.println( "         -recovery   --  to perform recovery on the store before checking" );
    }
}

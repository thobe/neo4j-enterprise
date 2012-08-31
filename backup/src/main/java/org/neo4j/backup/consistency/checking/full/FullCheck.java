/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.consistency.checking.full;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.progress.Completion;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
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
    private final ProgressMonitorFactory progressFactory;

    public FullCheck( boolean checkPropertyOwners, ProgressMonitorFactory progressFactory )
    {
        this.checkPropertyOwners = checkPropertyOwners;
        this.progressFactory = progressFactory;
    }

    public void execute( StoreAccess store, StringLogger logger ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyReporter.SummarisingReporter reporter = ConsistencyReporter.create( new SimpleRecordAccess( store ),
                                                                                       new DirectReferenceDispatcher(),
                                                                                       logger );
        execute( store, reporter );
        ConsistencySummaryStatistics summary = reporter.getSummary();
        if ( !summary.isConsistent() )
        {
            logger.logMessage( "Inconsistencies found: " + summary );
        }
    }

    public void execute( StoreAccess store, ConsistencyReport.Reporter reporter ) throws ConsistencyCheckIncompleteException

    {
        StoreProcessor processor = new StoreProcessor( checkPropertyOwners, reporter );

        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts( "Full consistency check" );
        List<StoreProcessorTask> tasks = new ArrayList<StoreProcessorTask>( 9 );

        tasks.add( new StoreProcessorTask<NodeRecord>( store.getNodeStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<RelationshipRecord>( store.getRelationshipStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<PropertyRecord>( store.getPropertyStore(), processor, progress ) );

        tasks.add( new StoreProcessorTask<RelationshipTypeRecord>( store.getRelationshipTypeStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<PropertyIndexRecord>( store.getPropertyIndexStore(), processor, progress ) );

        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getStringStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getArrayStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getTypeNameStore(), processor, progress ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getPropertyKeyStore(), processor, progress ) );

        Completion completion = progress.build();

        ExecutorService executor = execute( tasks );

        try
        {
            completion.await( 7, TimeUnit.DAYS );
        }
        catch ( Exception e )
        {
            processor.stopScanning();
            throw new ConsistencyCheckIncompleteException( e );
        }
        finally
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination( 10, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                // don't care
            }
        }

        processor.checkOrphanPropertyChains( progressFactory );
    }

    protected ExecutorService execute( List<? extends Runnable> tasks )
    {
        ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        for ( Runnable task : tasks )
        {
            executor.submit( task );
        }
        return executor;
    }

    public static void run( ProgressMonitorFactory progressFactory, String storeDir, Config config,
                            StringLogger logger ) throws ConsistencyCheckIncompleteException
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
        private final ProgressListener progressListener;

        StoreProcessorTask( RecordStore<R> store, StoreProcessor processor, ProgressMonitorFactory.MultiPartBuilder builder )
        {
            this.store = store;
            this.processor = processor;
            String name = store.getStorageFileName();
            this.progressListener = builder.progressForPart( name.substring( name.lastIndexOf( '/' ) + 1 ), store.getHighId() );
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run()
        {
            try
            {
                processor.applyFiltered( store, progressListener );
            }
            catch ( Throwable e )
            {
                progressListener.failed( e );
            }
        }
    }
}

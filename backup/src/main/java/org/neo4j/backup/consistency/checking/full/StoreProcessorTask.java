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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.progress.Completion;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

class StoreProcessorTask<R extends AbstractBaseRecord>
{
    private final RecordStore<R> store;
    private final StoreProcessor[] processors;
    private final ProgressListener progressListener;

    StoreProcessorTask( RecordStore<R> store, ProgressMonitorFactory.MultiPartBuilder builder,
                        StoreProcessor... processors )
    {
        this.store = store;
        this.processors = processors;
        String name = store.getStorageFileName();
        this.progressListener = builder
                .progressForPart( name.substring( name.lastIndexOf( '/' ) + 1 ), store.getHighId() );
    }

    public void multiPass()
    {
        for ( StoreProcessor processor : processors )
        {
            singlePass( processor );
        }
    }

    @SuppressWarnings("unchecked")
    public void singlePass( RecordStore.Processor processor )
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

    enum TaskExecution
    {
        MULTI_THREADED
        {
            @Override
            void execute( RecordStore.Processor processor, List<StoreProcessorTask> tasks, Completion completion )
                    throws ConsistencyCheckIncompleteException
            {
                ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
                for ( StoreProcessorTask task : tasks )
                {
                    executor.submit( new TaskRunner( processor, task ) );
                }

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
            }
        },
        SINGLE_THREADED
        {
            @Override
            void execute( RecordStore.Processor processor, List<StoreProcessorTask> tasks, Completion completion )
                    throws ConsistencyCheckIncompleteException
            {
                try
                {
                    for ( StoreProcessorTask task : tasks )
                    {
                        task.singlePass( processor );
                    }
                }
                catch ( Exception e )
                {
                    throw new ConsistencyCheckIncompleteException( e );
                }
            }
        },
        MULTI_PASS
        {
            @Override
            void execute( RecordStore.Processor processor, List<StoreProcessorTask> tasks, Completion completion )
                    throws ConsistencyCheckIncompleteException
            {
                try
                {
                    for ( StoreProcessorTask task : tasks )
                    {
                        task.multiPass();
                    }
                }
                catch ( Exception e )
                {
                    throw new ConsistencyCheckIncompleteException( e );
                }
            }
        };

        abstract void execute( RecordStore.Processor processor, List<StoreProcessorTask> tasks, Completion completion )
                throws ConsistencyCheckIncompleteException;
    }

    private static class TaskRunner implements Runnable
    {
        private final RecordStore.Processor processor;
        private final StoreProcessorTask task;

        public TaskRunner( RecordStore.Processor processor, StoreProcessorTask task )
        {
            this.processor = processor;
            this.task = task;
        }

        @Override
        public void run()
        {
            task.singlePass( processor );
        }
    }
}

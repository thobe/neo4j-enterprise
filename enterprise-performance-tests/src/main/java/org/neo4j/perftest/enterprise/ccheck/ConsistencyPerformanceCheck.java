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
package org.neo4j.perftest.enterprise.ccheck;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.neo4j.backup.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.backup.consistency.checking.full.FullCheck;
import org.neo4j.backup.consistency.checking.old.ConsistencyRecordProcessor;
import org.neo4j.backup.consistency.checking.old.ConsistencyReporter;
import org.neo4j.backup.consistency.checking.old.InconsistencyType;
import org.neo4j.backup.consistency.checking.old.MonitoringConsistencyReporter;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Parameters;
import org.neo4j.perftest.enterprise.util.Setting;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.perftest.enterprise.util.Configuration.SYSTEM_PROPERTIES;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;
import static org.neo4j.perftest.enterprise.util.Setting.booleanSetting;
import static org.neo4j.perftest.enterprise.util.Setting.enumSetting;
import static org.neo4j.perftest.enterprise.util.Setting.stringSetting;

public class ConsistencyPerformanceCheck
{
    private static final Setting<Boolean> generate_graph = booleanSetting( "generate_graph", false );
    private static final Setting<String> report_file = stringSetting( "report_file", "target/report.json" );
    private static final Setting<CheckerVersion> checker_version = enumSetting( "checker_version", CheckerVersion.NEW );
    private static final Setting<Boolean> wait_before_check = booleanSetting( "wait_before_check", false );
    private static final Setting<Boolean> single_threaded = booleanSetting( "single_threaded", false );

    private enum CheckerVersion
    {
        OLD
        {
            @Override
            void run( ProgressMonitorFactory progress, String storeDir, boolean singleThreaded )
            {
                new ConsistencyRecordProcessor(
                        new StoreAccess( storeDir ),
                        new MonitoringConsistencyReporter( new ConsistencyReporter()
                        {
                            @Override
                            public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                                    RecordStore<R1> recordStore,
                                    R1 record,
                                    RecordStore<? extends R2> referredStore,
                                    R2 referred,
                                    InconsistencyType inconsistency )
                            {
                            }

                            @Override
                            public <R extends AbstractBaseRecord> void report(
                                    RecordStore<R> recordStore, R record,
                                    InconsistencyType inconsistency )
                            {
                            }
                        } ), progress ).run();
            }
        },
        NEW
        {
            @Override
            void run( ProgressMonitorFactory progress, String storeDir, boolean singleThreaded ) throws ConsistencyCheckIncompleteException
            {
                FullCheck.run( progress, storeDir,
                               new Config( new ConfigurationDefaults( GraphDatabaseSettings.class )
                                                   .apply( stringMap(
                                                           FullCheck.consistency_check_single_threaded.name(),
                                                           Boolean.toString( singleThreaded ) ) ) ),
                               StringLogger.DEV_NULL );
            }
        };

        abstract void run( ProgressMonitorFactory progress, String storeDir, boolean singleThreaded )
                throws ConsistencyCheckIncompleteException;
    }

    /**
     * Sample execution:
     * java -cp ... org.neo4j.perftest.enterprise.ccheck.ConsistencyPerformanceCheck
     *    -generate_graph
     *    -report_file target/ccheck_performance.json
     *    -neo4j.store_dir target/ccheck_perf_graph
     *    -report_progress
     *    -node_count 10000000
     *    -relationships FOO:2,BAR:1
     *    -node_properties SINGLE_STRING,SINGLE_STRING,SINGLE_STRING,SINGLE_STRING,SINGLE_STRING
     */
    public static void main( String... args ) throws Exception
    {
        run( Parameters.configuration( SYSTEM_PROPERTIES,
                                       settingsOf( DataGenerator.class, ConsistencyPerformanceCheck.class ) )
                       .convert( args ) );
    }

    private static void run( Configuration configuration ) throws Exception
    {
        if ( configuration.get( generate_graph ) )
        {
            DataGenerator.run( configuration );
        }
        // ensure that the store is recovered
        new EmbeddedGraphDatabase( configuration.get( DataGenerator.store_dir ) ).shutdown();

        // run the consistency check
        ProgressMonitorFactory progress;
        if ( configuration.get( DataGenerator.report_progress ) )
        {
            progress = ProgressMonitorFactory.textual( System.out );
        }
        else
        {
            progress = ProgressMonitorFactory.NONE;
        }
        if ( configuration.get( wait_before_check ) )
        {
            System.out.println( "Press return to start the checker..." );
            System.in.read();
        }
        configuration.get( checker_version ).run( new TimingProgress( new JsonReportWriter( configuration ), progress ),
                                                  configuration.get( DataGenerator.store_dir ),
                                                  configuration.get( single_threaded ) );
    }

    private static class JsonReportWriter implements TimingProgress.Visitor
    {
        private final File target;
        private JsonGenerator json;
        private boolean writeRecordsPerSecond = true;
        private final Configuration configuration;

        JsonReportWriter( Configuration configuration )
        {
            this.configuration = configuration;
            target = new File( configuration.get( report_file ) );
        }

        @Override
        public void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException
        {
            ensureOpen( false );
            json = new JsonFactory().configure( JsonGenerator.Feature.AUTO_CLOSE_TARGET, true )
                                    .createJsonGenerator( new FileWriter( target ) );
            json.setPrettyPrinter( new DefaultPrettyPrinter() );
            json.writeStartObject();
            {
                json.writeFieldName( "config" );
                json.writeStartObject();
                emitConfig();
                json.writeEndObject();
            }
            {
                json.writeFieldName( "total" );
                json.writeStartObject();
                emitTime( totalElementCount, totalTimeNanos );
                json.writeEndObject();
            }
            json.writeFieldName( "phases" );
            json.writeStartArray();
        }

        private void emitConfig() throws IOException
        {
            for ( Setting<?> setting : settingsOf( DataGenerator.class, ConsistencyPerformanceCheck.class ) )
            {
                emitSetting( setting );
            }
        }

        private <T> void emitSetting( Setting<T> setting ) throws IOException
        {
            json.writeStringField( setting.name(), setting.asString( configuration.get( setting ) ) );
        }

        @Override
        public void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException
        {
            ensureOpen( true );
            json.writeStartObject();
            json.writeStringField( "name", phase );
            emitTime( elementCount, timeNanos );
            json.writeEndObject();
        }

        private void emitTime( long elementCount, long timeNanos ) throws IOException
        {
            json.writeNumberField( "elementCount", elementCount );
            double millis = nanosToMillis( timeNanos );
            json.writeNumberField( "time", millis );
            if ( writeRecordsPerSecond )
            {
                json.writeNumberField( "recordsPerSecond", (elementCount * 1000.0) / millis );
            }
        }

        @Override
        public void endTimingProgress() throws IOException
        {
            ensureOpen( true );
            json.writeEndArray();
            json.writeEndObject();
            json.close();
        }

        private static double nanosToMillis( long nanoTime )
        {
            return nanoTime / 1000000.0;
        }

        private void ensureOpen( boolean open ) throws IOException
        {
            if ( (json == null) == open )
            {
                throw new IOException(
                        new IllegalStateException( String.format( "Writing %s started.", open ? "not" : "already" ) ) );
            }
        }
    }
}

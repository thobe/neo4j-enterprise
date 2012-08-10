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
package org.neo4j.backup.consistency.check;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultLastCommittedTxIdSetter;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
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
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Finds inconsistency in a Neo4j store.
 *
 * Warning: will not find "dangling" records, i.e. records that are correct but
 * not referenced.
 *
 * Warning: will only find multiple references to the same property chain or
 * dynamic record chain for incremental checks (if the {@link RecordStore stores}
 * are {@link org.neo4j.backup.consistency.DiffRecordStore diff stores}). Also, this checking is very
 * incomplete.
 */
public abstract class ConsistencyCheck implements Iterable<RecordStore<?>>,
        ConsistencyReporter
{
    /** Defaults to false due to the way Boolean.parseBoolean(null) works. */
    private static final GraphDatabaseSetting<Boolean> consistency_check_property_owners =
            new GraphDatabaseSetting.BooleanSetting( "consistency_check_property_owners" );
    private final StoreAccess stores;

    /**
     * Run a full consistency check on the specified store.
     *
     * @param args The arguments to the checker, the first is taken as the path
     *            to the store to check.
     */
    public static void main( String... args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        boolean propowner = params.getBoolean( "propowner", false, true );
        boolean recovery = params.getBoolean( "recovery", false, true );
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
        if ( recovery )
        {
            new EmbeddedGraphDatabase( storeDir ).shutdown();
        }
        else
        {
            // TODO: the name of the active file is hard-coded, because there is no way to get it through code
            XaLogicalLogFiles logFiles = new XaLogicalLogFiles(
                    new File( storeDir, "nioneo_logical.log" ).getAbsolutePath(), new DefaultFileSystemAbstraction() );
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
        run( storeDir, new Config( new ConfigurationDefaults( GraphDatabaseSettings.class )
                                           .apply( stringMap( consistency_check_property_owners.name(),
                                                              Boolean.toString( propowner ) ) ) ) );
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines ) System.err.println( line );
        System.err.println( Args.jarUsage( ConsistencyCheck.class, "[-propowner] <storedir>" ) );
        System.err.println( "WHERE:   <storedir>  is the path to the store to check" );
        System.err.println( "         -propowner  --  to verify that properties are owned only once" );
        System.err.println( "         -recovery   --  to perform recovery on the store before checking" );
    }

    public static void run( String storeDir, Config config )
    {
        run( new ProgressIndicator.Textual( System.err ), storeDir, config );
    }

    public static void run( ProgressIndicator.Factory progressFactory, String storeDir, Config config )
    {
        StoreFactory factory = new StoreFactory( config,
                                                 new DefaultIdGeneratorFactory(),
                                                 new DefaultFileSystemAbstraction(),
                                                 new DefaultLastCommittedTxIdSetter(),
                                                 StringLogger.SYSTEM,
                                                 new DefaultTxHook() );
        NeoStore neoStore = factory.newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsolutePath() );
        try
        {
            StoreAccess store = new StoreAccess( neoStore );
            run( progressFactory, store, config.get( consistency_check_property_owners ) );
        }
        finally
        {
            neoStore.close();
        }
    }

    public static void run( StoreAccess stores, boolean propowner )
    {
        run( new ProgressIndicator.Textual( System.err ), stores, propowner );
    }

    public static void run( final ProgressIndicator.Factory progressFactory, StoreAccess stores, boolean propowner )
    {
        new ConsistencyCheck( stores, propowner )
        {
            @Override
            public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    InconsistencyType inconsistency )
            {
                System.out.println( record + " " + referred + " //" + inconsistency.message() );
            }

            @Override
            public <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record,
                    InconsistencyType inconsistency )
            {
                System.out.println( record + " //" + inconsistency.message() );
            }
        }.run(progressFactory);
    }

    private final RecordStore<NodeRecord> nodes;
    private final RecordStore<RelationshipRecord> rels;
    private final RecordStore<PropertyRecord> props;
    private final RecordStore<DynamicRecord> strings, arrays;
    private final RecordStore<PropertyIndexRecord>  propIndexes;
    private final RecordStore<RelationshipTypeRecord>  relTypes;
    private final RecordStore<DynamicRecord> propKeys;
    private final RecordStore<DynamicRecord> typeNames;
    @Override
    public Iterator<RecordStore<?>> iterator()
    {
        return Arrays.<RecordStore<?>>asList( nodes, rels, props, strings, arrays, propIndexes, relTypes, propKeys,
                typeNames ).iterator();
    }
    private final HashMap<Long/*property record id*/, PropertyOwner> propertyOwners;

    /**
     * Creates a standard checker or a checker that validates property owners.
     *
     * Property ownership validation validates that each property record is only
     * referenced once. This check has a bit of memory overhead.
     *
     * @param stores the stores to check.
     * @param checkPropertyOwners if <code>true</code> ownership validation will
     *            be performed.
     */
    public ConsistencyCheck( StoreAccess stores, boolean checkPropertyOwners )
    {
        this.stores = stores;
        this.nodes = stores.getNodeStore();
        this.rels = stores.getRelationshipStore();
        this.props = stores.getPropertyStore();
        this.strings = stores.getStringStore();
        this.arrays = stores.getArrayStore();
        this.relTypes = stores.getRelationshipTypeStore();
        this.propIndexes = stores.getPropertyIndexStore();
        this.propKeys = stores.getPropertyKeyStore();
        this.typeNames = stores.getTypeNameStore();
        this.propertyOwners = checkPropertyOwners ? new HashMap<Long, PropertyOwner>() : null;
    }

    boolean propowners()
    {
        return propertyOwners != null;
    }

    @SuppressWarnings( "unchecked" )
    public void run( ProgressIndicator.Factory progressFactory )
    {
        MonitoringConsistencyReporter monitor = new MonitoringConsistencyReporter( this );
        ConsistencyRecordProcessor processor = new ConsistencyRecordProcessor( stores, monitor, progressFactory );
        processor.run();
        monitor.checkResult();
    }
}

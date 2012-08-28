package org.neo4j.backup.consistency;

import java.io.File;
import java.io.IOException;

import org.neo4j.backup.consistency.check.full.FullCheck;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Progress;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogFiles;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Check
{
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
        FullCheck.run( Progress.textual( System.err ), storeDir,
                       new Config( new ConfigurationDefaults( GraphDatabaseSettings.class ).apply(
                               stringMap( FullCheck.consistency_check_property_owners.name(),
                                          Boolean.toString( params.getBoolean( "propowner", false, true ) ) ) ) ),
                       StringLogger.SYSTEM );
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

    private Check()
    {
        throw new UnsupportedOperationException( "No instances." );
    }
}

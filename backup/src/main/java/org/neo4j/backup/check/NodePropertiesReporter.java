package org.neo4j.backup.check;

import static java.lang.String.format;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class NodePropertiesReporter
{
    private final GraphDatabaseService database;

    public NodePropertiesReporter( GraphDatabaseService database )
    {
        this.database = database;
    }

    public void reportNodeProperties( PrintWriter writer, RecordSet<RelationshipRecord> relationshipRecords )
    {
        Set<Long> nodeIds = new HashSet<Long>();

        for ( RelationshipRecord relationshipRecord : relationshipRecords )
        {
            nodeIds.add( relationshipRecord.getFirstNode() );
            nodeIds.add( relationshipRecord.getSecondNode() );
        }

        for ( Long nodeId : nodeIds )
        {
            reportNodeProperties( writer, nodeId );
        }
    }

    private void reportNodeProperties( PrintWriter writer, Long nodeId )
    {
        try
        {
            Node node = database.getNodeById( nodeId );

            writer.println( String.format( "Properties for node %d", nodeId ) );
            for ( String propertyKey : node.getPropertyKeys() )
            {
                writer.println( String.format( "    %s = %s", propertyKey, node.getProperty( propertyKey ) ) );
            }
        }
        catch ( Exception e )
        {
            writer.println( format( "Failed to report properties for node %d:", nodeId ) );
            e.printStackTrace( writer );
        }
    }
}

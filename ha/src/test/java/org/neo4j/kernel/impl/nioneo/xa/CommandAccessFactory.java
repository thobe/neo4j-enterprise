package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class CommandAccessFactory
{
    public static Command command( RelationshipRecord relationship )
    {
        return new Command.RelationshipCommand( null, relationship );
    }

    public static Command command( NodeRecord node )
    {
        return new Command.NodeCommand( null, node );
    }

    public static Command command( PropertyRecord property )
    {
        return new Command.PropertyCommand( null, property );
    }
}

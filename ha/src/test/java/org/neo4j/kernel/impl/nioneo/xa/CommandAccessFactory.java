package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class CommandAccessFactory
{
    public static Command relationship( long id, long firstNode, long secondNode, int type,
                                        long firstNextRel, long firstPrevRel, long secondNextRel, long secondPrevRel )
    {
        RelationshipRecord relationship = new RelationshipRecord( id, firstNode, secondNode, type );
        relationship.setFirstNextRel( firstNextRel );
        relationship.setFirstPrevRel( firstPrevRel );
        relationship.setSecondNextRel( secondNextRel );
        relationship.setSecondPrevRel( secondPrevRel );
        return new Command.RelationshipCommand( null, relationship );
    }
}

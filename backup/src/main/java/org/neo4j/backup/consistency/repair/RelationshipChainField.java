package org.neo4j.backup.consistency.repair;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_NEXT_DIFFERENT_CHAIN;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_NEXT_NOT_IN_USE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_NO_BACKREF;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_PREV_DIFFERENT_CHAIN;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.SOURCE_PREV_NOT_IN_USE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_NEXT_DIFFERENT_CHAIN;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_NEXT_NOT_IN_USE;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_NO_BACKREF;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_PREV_DIFFERENT_CHAIN;
import static org.neo4j.backup.consistency.InconsistencyType.ReferenceInconsistency.TARGET_PREV_NOT_IN_USE;

@SuppressWarnings( "boxing" )
public enum RelationshipChainField
{
    FIRST_NEXT( true, Record.NO_NEXT_RELATIONSHIP, SOURCE_NEXT_NOT_IN_USE, null, SOURCE_NEXT_DIFFERENT_CHAIN )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstNextRel();
        }

        @Override
        public boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
        {
            long node = getNode( rel );
            if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
            if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
            return false;
        }
    },
    FIRST_PREV( true, Record.NO_PREV_RELATIONSHIP, SOURCE_PREV_NOT_IN_USE, SOURCE_NO_BACKREF,
            SOURCE_PREV_DIFFERENT_CHAIN )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getFirstPrevRel();
        }

        @Override
        public Long nodeOf( RelationshipRecord rel )
        {
            return getNode( rel );
        }

        @Override
        public boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
        {
            long node = getNode( rel );
            if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
            if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
            return false;
        }
    },
    SECOND_NEXT( false, Record.NO_NEXT_RELATIONSHIP, TARGET_NEXT_NOT_IN_USE, null, TARGET_NEXT_DIFFERENT_CHAIN )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondNextRel();
        }

        @Override
        public boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
        {
            long node = getNode( rel );
            if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
            if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
            return false;
        }
    },
    SECOND_PREV( false, Record.NO_PREV_RELATIONSHIP, TARGET_PREV_NOT_IN_USE, TARGET_NO_BACKREF,
            TARGET_PREV_DIFFERENT_CHAIN )
    {
        @Override
        public long relOf( RelationshipRecord rel )
        {
            return rel.getSecondPrevRel();
        }

        @Override
        public Long nodeOf( RelationshipRecord rel )
        {
            return getNode( rel );
        }

        @Override
        public boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
        {
            long node = getNode( rel );
            if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
            if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
            return false;
        }
    };

    public final InconsistencyType.ReferenceInconsistency notInUse, noBackReference, differentChain;
    private final boolean first;
    public final long none;

    RelationshipChainField( boolean first, Record none, InconsistencyType.ReferenceInconsistency notInUse,
                            InconsistencyType.ReferenceInconsistency noBackReference,
                            InconsistencyType.ReferenceInconsistency differentChain )
    {
        this.first = first;
        this.none = none.intValue();
        this.notInUse = notInUse;
        this.noBackReference = noBackReference;
        this.differentChain = differentChain;
    }

    public abstract boolean invConsistent( RelationshipRecord rel, RelationshipRecord other );

    long getNode( RelationshipRecord rel )
    {
        return first ? rel.getFirstNode() : rel.getSecondNode();
    }

    public abstract long relOf( RelationshipRecord rel );

    public Long nodeOf( RelationshipRecord rel )
    {
        return null;
    }

}

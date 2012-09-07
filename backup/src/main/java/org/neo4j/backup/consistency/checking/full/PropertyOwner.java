package org.neo4j.backup.consistency.checking.full;

import org.neo4j.backup.consistency.report.PendingReferenceCheck;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

abstract class PropertyOwner<RECORD extends PrimitiveRecord>
{
    abstract RecordReference<RECORD> record( RecordAccess records );

    void checkOrphanage()
    {
        // default: do nothing
    }

    static class OwningNode extends PropertyOwner<NodeRecord>
    {
        private final long id;

        OwningNode( long id )
        {
            this.id = id;
        }

        @Override
        RecordReference<NodeRecord> record( RecordAccess records )
        {
            return records.node( id );
        }
    }

    static class OwningRelationship extends PropertyOwner<RelationshipRecord>
    {
        private final long id;

        OwningRelationship( long id )
        {
            this.id = id;
        }

        @Override
        RecordReference<RelationshipRecord> record( RecordAccess records )
        {
            return records.relationship( id );
        }
    }

    static class UnknownOwner extends PropertyOwner<PrimitiveRecord> implements RecordReference<PrimitiveRecord>
    {
        private PendingReferenceCheck<PrimitiveRecord> reporter;

        @Override
        RecordReference<PrimitiveRecord> record( RecordAccess records )
        {
            this.skip();
            return SKIP;
        }

        @Override
        void checkOrphanage()
        {
            PendingReferenceCheck<PrimitiveRecord> reporter;
            synchronized ( this )
            {
                reporter = this.reporter;
                this.reporter = null;
            }
            if ( reporter != null )
            {
                reporter.checkReference( null );
            }
        }

        synchronized void skip()
        {
            if ( reporter != null )
            {
                reporter.skip();
                reporter = null;
            }
        }

        @Override
        public synchronized void dispatch( PendingReferenceCheck<PrimitiveRecord> reporter )
        {
            this.reporter = reporter;
        }
    }

    private static final RecordReference<PrimitiveRecord> SKIP = new RecordReference<PrimitiveRecord>()
    {
        @Override
        public void dispatch( PendingReferenceCheck<PrimitiveRecord> reporter )
        {
            reporter.skip();
        }
    };
}

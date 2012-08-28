package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RelationshipRecordCheck
        extends PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    RelationshipRecordCheck()
    {
        super( Label.FIELD,
               RelationshipNodeField.SOURCE, RelationshipField.SOURCE_PREV, RelationshipField.SOURCE_NEXT,
               RelationshipNodeField.TARGET, RelationshipField.TARGET_PREV, RelationshipField.TARGET_NEXT );
    }

    @Override
    PropertyOwner owner( RelationshipRecord record )
    {
        return new PropertyOwner.OwningRelationship( record.getId() );
    }

    private enum Label implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipTypeRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        FIELD;

        @Override
        public void checkConsistency( RelationshipRecord record, ConsistencyReport.RelationshipConsistencyReport report,
                                      RecordReferencer records )
        {
            if ( record.getType() < 0 )
            {
                report.illegalLabel();
            }
            else
            {
                report.forReference( records.relationshipLabel( record.getType() ), this );
            }
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipTypeRecord referred,
                                    ConsistencyReport.RelationshipConsistencyReport report )
        {
            if ( !referred.inUse() )
            {
                report.labelNotInUse( referred );
            }
        }
    }

    private enum RelationshipField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        SOURCE_PREV( RelationshipNodeField.SOURCE, Record.NO_PREV_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstPrevRel();
            }

            @Override
            long other( RelationshipNodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourcePrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.sourcePrevDoesNotReferenceBack( relationship );
            }
        },
        SOURCE_NEXT( RelationshipNodeField.SOURCE, Record.NO_NEXT_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstNextRel();
            }

            @Override
            long other( RelationshipNodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourceNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.sourceNextDoesNotReferenceBack( relationship );
            }
        },
        TARGET_PREV( RelationshipNodeField.TARGET, Record.NO_PREV_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondPrevRel();
            }

            @Override
            long other( RelationshipNodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetPrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.targetPrevDoesNotReferenceBack( relationship );
            }
        },
        TARGET_NEXT( RelationshipNodeField.TARGET, Record.NO_NEXT_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondNextRel();
            }

            @Override
            long other( RelationshipNodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.targetNextDoesNotReferenceBack( relationship );
            }
        };
        private final RelationshipNodeField NODE;
        private final Record NONE;

        private RelationshipField( RelationshipNodeField node, Record none )
        {
            this.NODE = node;
            this.NONE = none;
        }

        public abstract long valueFrom( RelationshipRecord relationship );

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      ConsistencyReport.RelationshipConsistencyReport report, RecordReferencer records )
        {
            if ( !NONE.is( valueFrom( relationship ) ) )
            {
                report.forReference( records.relationship( valueFrom( relationship ) ), this );
            }
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipRecord referred,
                                    ConsistencyReport.RelationshipConsistencyReport report )
        {
            RelationshipNodeField field = RelationshipNodeField.select( referred, node( record ) );
            if ( field == null )
            {
                otherNode( report, referred );
            }
            else
            {
                if ( other( field, referred ) != record.getId() )
                {
                    noBackReference( report, referred );
                }
            }
        }

        abstract long other( RelationshipNodeField field, RelationshipRecord relationship );

        abstract void otherNode( ConsistencyReport.RelationshipConsistencyReport report,
                                 RelationshipRecord relationship );

        abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                       RelationshipRecord relationship );

        private long node( RelationshipRecord relationship )
        {
            return NODE.node( relationship );
        }
    }
}

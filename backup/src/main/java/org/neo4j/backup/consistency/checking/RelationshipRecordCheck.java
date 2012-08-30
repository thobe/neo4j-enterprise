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
package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RelationshipRecordCheck
        extends PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    public RelationshipRecordCheck()
    {
        super( Label.FIELD,
               RelationshipNodeField.SOURCE, RelationshipField.SOURCE_PREV, RelationshipField.SOURCE_NEXT,
               RelationshipNodeField.TARGET, RelationshipField.TARGET_PREV, RelationshipField.TARGET_NEXT );
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
        public long valueFrom( RelationshipRecord record )
        {
            return record.getType();
        }

        @Override
        public boolean isNone( RelationshipRecord record )
        {
            return false;
        }

        @Override
        public boolean referencedRecordChanged( DiffRecordReferencer records, RelationshipRecord record )
        {
            return false;
        }

        @Override
        public void reportReplacedButNotUpdated( ConsistencyReport.RelationshipConsistencyReport report )
        {
            // do nothing
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

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.sourcePrevReplacedButNotUpdated();
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

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.sourceNextReplacedButNotUpdated();
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

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.targetPrevReplacedButNotUpdated();
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

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.targetNextReplacedButNotUpdated();
            }
        };
        private final RelationshipNodeField NODE;
        private final Record NONE;

        private RelationshipField( RelationshipNodeField node, Record none )
        {
            this.NODE = node;
            this.NONE = none;
        }

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      ConsistencyReport.RelationshipConsistencyReport report, RecordReferencer records )
        {
            if ( !isNone( relationship ) )
            {
                report.forReference( records.relationship( valueFrom( relationship ) ), this );
            }
        }

        @Override
        public boolean isNone( RelationshipRecord record )
        {
            return NONE.is( valueFrom( record ) );
        }

        @Override
        public boolean referencedRecordChanged( DiffRecordReferencer records, RelationshipRecord record )
        {
            return records.changedRelationship( valueFrom( record ) ) != null;
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
            return NODE.valueFrom( relationship );
        }
    }
}

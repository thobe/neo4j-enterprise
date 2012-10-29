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
package org.neo4j.consistency.repair.tool;

import java.util.List;

import org.neo4j.consistency.repair.RecordSet;
import org.neo4j.consistency.repair.RelationshipChainExplorer;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class RelationshipRemovalStrategy
{
    private final RecordRemover remover;
    private final RecordAccess recordAccess;
    private final Diagnostics diagnostics;

    public RelationshipRemovalStrategy( RecordAccess recordAccess, RecordRemover remover, Diagnostics diagnostics )
    {
        this.remover = remover;
        this.recordAccess = recordAccess;
        this.diagnostics = diagnostics;
    }

    public void evaluate( List<RelationshipRecord> relationshipRecords )
    {
        RelationshipChainExplorer explorer = new RelationshipChainExplorer( recordAccess );

        RecordSet<RelationshipRecord> toBeDeleted = new RecordSet<RelationshipRecord>();

        for ( RelationshipRecord inconsistentRecord : relationshipRecords )
        {
            RecordSet<RelationshipRecord> candidateRecords = new RecordSet<RelationshipRecord>();
            candidateRecords.addAll( explorer.exploreRelationshipRecordChainsToDepthTwo( inconsistentRecord ) );

            boolean abort = false;

            for ( RelationshipRecord relationshipRecord : candidateRecords )
            {
                NodeRecord firstNode = recordAccess.node( relationshipRecord.getFirstNode() ).forceLoad();
                NodeRecord secondNode = recordAccess.node( relationshipRecord.getSecondNode() ).forceLoad();

                if ( firstNode.getNextRel() == relationshipRecord.getId() )
                {
                    diagnostics.removalOfRelationshipsPreventedBy( firstNode, candidateRecords );
                    abort = true;
                }
                else if ( secondNode.getNextRel() == relationshipRecord.getId() )
                {
                    diagnostics.removalOfRelationshipsPreventedBy( secondNode, candidateRecords );
                    abort = true;
                }
            }

            if ( !abort )
            {
                toBeDeleted.addAll( candidateRecords );
            }
        }
        for ( RelationshipRecord relationshipRecord : toBeDeleted )
        {
            remover.remove( relationshipRecord );
        }
    }
}

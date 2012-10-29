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

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.MessageConsistencyLogger;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.util.StringLogger;

@Ignore("in progress")
public class ConsistencyCheckOutputParserTest
{
    @Test
    public void shouldParseOutput() throws Exception
    {
        // given
        StringWriter log = new StringWriter();
        MessageConsistencyLogger logger = new MessageConsistencyLogger( StringLogger.wrap( log ) );
        logger.error( RecordType.NODE, new NodeRecord( 101, -1, -1 ), "Some inconsistency" );

        ConsistencyCheckOutputParser parser = new ConsistencyCheckOutputParser();

        // when
        ConsistencyCheckOutput output = parser.parse( new StringReader( log.toString() ) );

        // then
        assertEquals( 1, output.getNodeRecords().size() );
        assertEquals( 0, output.getRelationshipRecords().size() );
        assertEquals( 0, output.getPropertyRecords().size() );
    }
}

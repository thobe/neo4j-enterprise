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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.MessageConsistencyLogger;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;
import static org.neo4j.consistency.repair.RecordSet.asSet;

public class ConsistencyCheckOutputParserTest
{
    // given
    private final StringWriter log = new StringWriter();
    private final RecordAccessStub records = new RecordAccessStub();
    private final MessageConsistencyLogger logger = new MessageConsistencyLogger( StringLogger.wrap( log ) );

    private ConsistencyCheckOutput parse() throws IOException
    {
        // given
        ConsistencyCheckOutputParser.Warnings warnings = mock( ConsistencyCheckOutputParser.Warnings.class );
        // when
        ConsistencyCheckOutput result = parse( warnings );
        // then
        verifyZeroInteractions( warnings );
        return result;
    }

    private ConsistencyCheckOutput parse( ConsistencyCheckOutputParser.Warnings warnings ) throws IOException
    {
        // given
        ConsistencyCheckOutputParser parser = new ConsistencyCheckOutputParser( records, warnings );
        StringReader reader = new StringReader( log.toString() );

        // parse
        return parser.parse( reader );
    }

    @Test
    public void shouldParseNode() throws Exception
    {
        // given
        NodeRecord record = add( inUse( new NodeRecord( 101, -1, -1 ) ), "Some inconsistency" );

        // when
        ConsistencyCheckOutput output = parse();

        // then
        assertEquals( asSet( record ), output.getNodeRecords() );
        assertEquals( 0, output.getRelationshipRecords().size() );
        assertEquals( 0, output.getPropertyRecords().size() );
    }

    @Test
    public void shouldParseRelationship() throws Exception
    {
        // given
        RelationshipRecord record = add( inUse( new RelationshipRecord( 101, 1, 2, 1001 ) ), "Some inconsistency" );

        // when
        ConsistencyCheckOutput output = parse();

        // then
        assertEquals( 0, output.getNodeRecords().size() );
        assertEquals( asSet( record ), output.getRelationshipRecords() );
        assertEquals( 0, output.getPropertyRecords().size() );
    }

    @Test
    public void shouldParseProperty() throws Exception
    {
        // given
        PropertyRecord record = add( inUse( new PropertyRecord( 101 ) ), "Some inconsistency" );

        // when
        ConsistencyCheckOutput output = parse();

        // then
        assertEquals( 0, output.getNodeRecords().size() );
        assertEquals( 0, output.getRelationshipRecords().size() );
        assertEquals( asSet( record ), output.getPropertyRecords() );
    }

    @Test
    public void shouldParseMultipleRecords() throws Exception
    {
        // given
        NodeRecord node1 = add( inUse( new NodeRecord( 11, -1, -1 ) ), "Some inconsistency" );
        NodeRecord node2 = add( inUse( new NodeRecord( 12, -1, -1 ) ),
                                notInUse( new NodeRecord( 12, -1, -1 ) ), "Some inconsistency" );
        RelationshipRecord rel1 = add( inUse( new RelationshipRecord( 101, 1, 2, 1001 ) ), "Some inconsistency" );
        RelationshipRecord rel2 = add( inUse( new RelationshipRecord( 102, 1, 2, 1001 ) ),
                                       notInUse( new RelationshipRecord( 102, 0, 0, 0 ) ), "Some inconsistency" );
        PropertyRecord prop1 = add( inUse( new PropertyRecord( 1001 ) ), "Some inconsistency" );
        PropertyRecord prop2 = add( inUse( new PropertyRecord( 1002 ) ),
                                    notInUse( new PropertyRecord( 1002 ) ), "Some inconsistency" );

        // when
        ConsistencyCheckOutput output = parse();

        // then
        assertEquals( asSet( node1, node2 ), output.getNodeRecords() );
        assertEquals( asSet( rel1, rel2 ), output.getRelationshipRecords() );
        assertEquals( asSet( prop1, prop2 ), output.getPropertyRecords() );
    }

    @Test
    public void shouldWarnIfLoadedNodeDoesNotMatchLogLine() throws Exception
    {
        // given
        NodeRecord record = inUse( new NodeRecord( 101, -1, -1 ) );
        String pre = record.toString();
        add( record, "Inconsistent" ).setInUse( false );
        ConsistencyCheckOutputParser.Warnings warnings = mock( ConsistencyCheckOutputParser.Warnings.class );

        // when
        parse( warnings );

        // then
        verify( warnings ).changedRecord( record, pre );
        verifyNoMoreInteractions( warnings );
    }

    @Test
    public void shouldWarnIfLoadedRelationshipDoesNotMatchLogLine() throws Exception
    {
        // given
        RelationshipRecord record = inUse( new RelationshipRecord( 101, 1, 2, 1001 ) );
        String pre = record.toString();
        add( record, "Inconsistent" ).setInUse( false );
        ConsistencyCheckOutputParser.Warnings warnings = mock( ConsistencyCheckOutputParser.Warnings.class );

        // when
        parse( warnings );

        // then
        verify( warnings ).changedRecord( record, pre );
        verifyNoMoreInteractions( warnings );
    }

    @Test
    public void shouldWarnIfLoadedPropertyDoesNotMatchLogLine() throws Exception
    {
        // given
        PropertyRecord record = inUse( new PropertyRecord( 101 ) );
        String pre = record.toString();
        add( record, "Inconsistent" ).setInUse( false );
        ConsistencyCheckOutputParser.Warnings warnings = mock( ConsistencyCheckOutputParser.Warnings.class );

        // when
        parse( warnings );

        // then
        verify( warnings ).changedRecord( record, pre );
        verifyNoMoreInteractions( warnings );
    }

    private NodeRecord add( NodeRecord record, String message, Object... args )
    {
        records.add( record );
        logger.error( RecordType.NODE, record, message, args );
        return record;
    }

    private NodeRecord add( NodeRecord oldRecord, NodeRecord newRecord, String message, Object... args )
    {
        records.add( newRecord );
        logger.error( RecordType.NODE, oldRecord, newRecord, message, args );
        return newRecord;
    }

    private RelationshipRecord add( RelationshipRecord record, String message, Object... args )
    {
        records.add( record );
        logger.error( RecordType.RELATIONSHIP, record, message, args );
        return record;
    }

    private RelationshipRecord add( RelationshipRecord oldRecord, RelationshipRecord newRecord, String message,
                                    Object... args )
    {
        records.add( newRecord );
        logger.error( RecordType.RELATIONSHIP, oldRecord, newRecord, message, args );
        return newRecord;
    }

    private PropertyRecord add( PropertyRecord record, String message, Object... args )
    {
        records.add( record );
        logger.error( RecordType.PROPERTY, record, message, args );
        return record;
    }

    private PropertyRecord add( PropertyRecord oldRecord, PropertyRecord newRecord, String message, Object... args )
    {
        records.add( newRecord );
        logger.error( RecordType.PROPERTY, oldRecord, newRecord, message, args );
        return newRecord;
    }
}

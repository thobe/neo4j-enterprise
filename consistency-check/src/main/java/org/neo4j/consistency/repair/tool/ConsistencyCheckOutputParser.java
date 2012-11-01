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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.consistency.repair.RecordSet;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class ConsistencyCheckOutputParser
{
    public interface Warnings
    {
        void changedRecord( AbstractBaseRecord record, String expectedString );
    }

    private static final int RECORD = 1;
    private static final int TYPE = 2;
    private static final int ID = 3;
    private static final Pattern PATTERN = Pattern// RECORD ::= TYPE[ID,...]
            .compile( "^\\t?\\+? ?((Node|Relationship|Property)(?:Record)?\\[([0-9]+),.*\\])$" );
    private final RecordAccess records;
    private final Warnings warnings;

    public ConsistencyCheckOutputParser( RecordAccess records, Warnings warnings )
    {
        this.records = records;
        this.warnings = warnings;
    }

    private enum RecordType
    {
        Node
        {
            @Override
            AbstractBaseRecord load( ConsistencyCheckOutput result, RecordAccess records, long id )
            {
                return add( result.getNodeRecords(), records.node( id ) );
            }
        },
        Relationship
        {
            @Override
            AbstractBaseRecord load( ConsistencyCheckOutput result, RecordAccess records, long id )
            {
                return add( result.getRelationshipRecords(), records.relationship( id ) );
            }
        },
        Property
        {
            @Override
            AbstractBaseRecord load( ConsistencyCheckOutput result, RecordAccess records, long id )
            {
                return add( result.getPropertyRecords(), records.property( id ) );
            }
        };

        private static <R extends Abstract64BitRecord> R add( RecordSet<R> recordSet, RecordReference<R> reference )
        {
            R record = reference.forceLoad();
            recordSet.add( record );
            return record;
        }

        abstract AbstractBaseRecord load( ConsistencyCheckOutput result, RecordAccess records, long id );
    }

    public ConsistencyCheckOutput parse( Reader reader ) throws IOException
    {
        BufferedReader br = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader( reader );
        ConsistencyCheckOutput result = new ConsistencyCheckOutput();
        for ( String line; null != (line = br.readLine()); )
        {
            Matcher matcher = PATTERN.matcher( line );
            if ( matcher.find() )
            {
                RecordType type = RecordType.valueOf( matcher.group( TYPE ) );
                long id = Long.parseLong( matcher.group( ID ) );
                AbstractBaseRecord record = type.load( result, records, id );
                line = matcher.group( RECORD );
                if ( !line.equals( record.toString() ) )
                {
                    warnings.changedRecord( record, line );
                }
            }
        }
        return result;
    }
}

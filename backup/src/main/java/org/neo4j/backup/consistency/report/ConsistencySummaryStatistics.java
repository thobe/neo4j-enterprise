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
package org.neo4j.backup.consistency.report;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.backup.consistency.RecordType;

public class ConsistencySummaryStatistics
{
    private final ConcurrentMap<RecordType, AtomicInteger> inconsistentRecordCount =
            new ConcurrentHashMap<RecordType, AtomicInteger>();
    private final AtomicInteger totalInconsistencyCount = new AtomicInteger();
    private final AtomicLong errorCount = new AtomicLong(), warningCount = new AtomicLong();

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() ).append( '{' );
        result.append( "\n\tNumber of errors: " ).append( errorCount );
        result.append( "\n\tNumber of warnings: " ).append( warningCount );
        for ( Map.Entry<RecordType, AtomicInteger> entry : inconsistentRecordCount.entrySet() )
        {
            result.append( "\n\tNumber of inconsistent " )
                  .append( entry.getKey() ).append( " records: " ).append( entry.getValue() );
        }
        return result.append( "\n}" ).toString();
    }

    public boolean isConsistent()
    {
        return totalInconsistencyCount.get() == 0;
    }

    public int getInconsistencyCountForRecordType( RecordType recordType )
    {
        AtomicInteger counter = inconsistentRecordCount.get( recordType );
        return counter == null ? 0 : counter.get();
    }

    public int getTotalInconsistencyCount()
    {
        return totalInconsistencyCount.get();
    }

    void add( RecordType recordType, int errors, int warnings )
    {
        if ( errors > 0 )
        {
            AtomicInteger counter = inconsistentRecordCount.get( recordType );
            if ( counter == null )
            {
                AtomicInteger suggestion = new AtomicInteger();
                counter = inconsistentRecordCount.putIfAbsent( recordType, suggestion );
                if ( counter == null )
                {
                    counter = suggestion;
                }
            }
            counter.incrementAndGet();
            totalInconsistencyCount.incrementAndGet();
        }
        errorCount.addAndGet( errors );
        warningCount.addAndGet( warnings );
    }
}

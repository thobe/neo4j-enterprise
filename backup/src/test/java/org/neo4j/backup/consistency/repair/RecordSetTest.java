package org.neo4j.backup.consistency.repair;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import static org.junit.Assert.assertEquals;

public class RecordSetTest
{
    @Test
    public void toStringShouldPlaceEachRecordOnItsOwnLine() throws Exception
    {
        // given
        NodeRecord record1 = new NodeRecord( 1, 1, 1 );
        NodeRecord record2 = new NodeRecord( 2, 2, 2 );
        RecordSet<NodeRecord> set = new RecordSet<NodeRecord>();
        set.add( record1 );
        set.add( record2 );

        // when
        String string = set.toString();

        // then
        String[] lines = string.split( "\n" );
        assertEquals(4, lines.length);
        assertEquals( "[", lines[0] );
        assertEquals( record1.toString() + ",", lines[1] );
        assertEquals( record2.toString() + ",", lines[2] );
        assertEquals( "]", lines[3] );
    }
}

package org.neo4j.backup.check;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;

class RecordSet<R extends Abstract64BitRecord> implements Iterable<R>
{
    private Map<Long, R> map = new HashMap<Long, R>();

    void add(R record)
    {
        map.put( record.getId(), record );
    }

    RecordSet<R> union(RecordSet<R> other)
    {
        RecordSet<R> set = new RecordSet<R>();
        set.addAll( this );
        set.addAll( other );
        return set;
    }

    int size() {
        return map.size();
    }

    @Override
    public Iterator<R> iterator()
    {
        return map.values().iterator();
    }

    public void addAll( RecordSet<R> other )
    {
        for ( R record : other.map.values() )
        {
            add( record );
        }
    }
}

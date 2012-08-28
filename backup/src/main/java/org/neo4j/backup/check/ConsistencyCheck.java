package org.neo4j.backup.check;

import org.neo4j.backup.consistency.Check;

/** @deprecated use {@link org.neo4j.backup.consistency.Check} instead. */
@Deprecated
class ConsistencyCheck
{
    @SuppressWarnings("deprecation")
    public static void main( String[] args )
    {
        System.err.printf( "WARNING: %s has been deprecated, please use %s instead.%n",
                           ConsistencyCheck.class.getName(), Check.class.getName() );
        Check.main( args );
    }
}

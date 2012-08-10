package org.neo4j.backup.check;

/**
 * @deprecated use {@link org.neo4j.backup.consistency.check.ConsistencyCheck} instead.
 */
@Deprecated
class ConsistencyCheck
{
    @SuppressWarnings( "deprecation" )
    public static void main( String[] args )
    {
        System.err.printf( "WARNING: %s has been deprecated please use %s instead.%n",
                ConsistencyCheck.class.getName(),
                org.neo4j.backup.consistency.check.ConsistencyCheck.class.getName() );
        org.neo4j.backup.consistency.check.ConsistencyCheck.main( args );
    }
}

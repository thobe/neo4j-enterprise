package org.neo4j.backup.consistency.checking.full;

public class ConsistencyCheckIncompleteException extends Exception
{
    public ConsistencyCheckIncompleteException( Exception cause )
    {
        super( "Full consistency check did not complete", cause );
    }
}

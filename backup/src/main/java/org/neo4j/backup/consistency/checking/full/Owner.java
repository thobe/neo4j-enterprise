package org.neo4j.backup.consistency.checking.full;

interface Owner
{
    void checkOrphanage();
}

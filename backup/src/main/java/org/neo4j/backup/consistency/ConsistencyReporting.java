package org.neo4j.backup.consistency;

import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

@Deprecated
public interface ConsistencyReporting
{
    ConsistencyReport.NodeConsistencyReport forNode( NodeRecord node );
}

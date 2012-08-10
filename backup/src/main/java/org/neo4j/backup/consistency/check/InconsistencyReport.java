/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

public interface InconsistencyReport
{
    // Inconsistency between two records
    <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred, InconsistencyType type );

    <R extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R> store, R record, R referred, InconsistencyType type ) ;

    // Internal inconsistency in a single record
    <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, InconsistencyType type );

}

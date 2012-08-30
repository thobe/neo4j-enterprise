/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

@Deprecated
public interface ConsistencyReporter
{
    /**
     * Report an inconsistency between two records.
     *
     * @param recordStore the store containing the record found to be inconsistent.
     * @param record the record found to be inconsistent.
     * @param referredStore the store containing the record the inconsistent record references.
     * @param referred the record the inconsistent record references.
     * @param inconsistency a description of the inconsistency.
     */
    <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
            InconsistencyType inconsistency );

    /**
     * Report an internal inconsistency in a single record.
     *
     * @param recordStore the store the inconsistent record is stored in.
     * @param record the inconsistent record.
     * @param inconsistency a description of the inconsistency.
     */
    <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record, InconsistencyType inconsistency );
}

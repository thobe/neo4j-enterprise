package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class TestApplyBrokenTransactionsOnSlave
{
    @Rule
    public final FakeMasterFixture fixture = new FakeMasterFixture();

    @Test
    public void shouldApplyValidTransactionsWithoutError() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase db = fixture.startDatabase( FakeMasterFixture.EMPTY_INITIAL_DATABASE );

        fixture.transaction( new FakeMasterFixture.MasterTransaction()
        {
            @Override
            public void transactionData( FakeMasterFixture.TransactionCommandFactory tx )
            {
                tx.add( new RelationshipRecord( 0, 0, 0, 0 ) );
            }
        } );

        // when
        db.pullUpdates();

        // then
        verifyThatUpdatesHaveBeenApplied();
    }

    @Test
    public void shouldTriggerRecoveryAfterApplyingATransactionFailsDueToRuntimeException() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase db = fixture.startDatabase( FakeMasterFixture.EMPTY_INITIAL_DATABASE );

        fixture.transaction( new FakeMasterFixture.MasterTransaction()
        {
            @Override
            public void transactionData( FakeMasterFixture.TransactionCommandFactory tx )
            {
                tx.add( new RelationshipRecord( -100, 0, 0, 0 ) );
            }
        } );

        // when
        try
        {
            db.pullUpdates();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        // then
        verifyThatRecoveryHasTakenPlace();
    }

    private void verifyThatRecoveryHasTakenPlace()
    {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void verifyThatUpdatesHaveBeenApplied()
    {
    }

}

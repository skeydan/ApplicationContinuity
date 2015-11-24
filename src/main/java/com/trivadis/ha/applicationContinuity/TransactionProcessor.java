package com.trivadis.ha.applicationContinuity;

import java.sql.SQLException;

interface TransactionProcessor {
    boolean process(Transaction transaction) throws SQLException;
}

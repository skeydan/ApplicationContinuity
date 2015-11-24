package com.trivadis.ha.applicationContinuity;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.replay.OracleDataSource;
import oracle.jdbc.replay.OracleDataSourceFactory;
import oracle.jdbc.OracleConnection;

class ManualTransactionProcessor extends AbstractTransactionProcessor {

    private static final Logger logger = Logger.getLogger(ManualTransactionProcessor.class.getName());
    private static final OracleDataSource rds = OracleDataSourceFactory.getOracleDataSource();

    protected ManualTransactionProcessor(String url, String user, String passwd) throws IOException {

        getProperties();
        try {
            rds.setUser(user);
            rds.setPassword(passwd);
            rds.setURL(url);
        } catch (SQLException ex) {
            Logger.getLogger(UCPTransactionProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    protected Connection getConnection() throws SQLException {

        Connection conn = null;
        while (conn == null) {
            try {
                conn = rds.getConnection();
            } catch (SQLException r) {
                logger.severe("Could not connect: " + r.getMessage());
            }
        }
        conn.setClientInfo("OCSID.MODULE", moduleName);
        return conn;
    }

    @Override
    public boolean process(Transaction transaction) throws SQLException {

        Connection conn = getConnection();

        DemoHelper.logConnectedSessions(getConnection(), serviceName);
        DemoHelper.logSessionToKill(getConnection(), serviceName, appUser);

        if (autocommit_outside_request) {
            conn.setAutoCommit(false);
        }

        logger.info("Begin request.");
        ((OracleConnection) conn).beginRequest();

        if (!autocommit_outside_request) {
            conn.setAutoCommit(false);
        }

        DemoHelper.ORA_41412_sysdate(conn);

        if (ORA_41412_session) {
            DemoHelper.ORA_41412_session(conn);
        }

        transaction.execute(conn);

        logger.info("Work done. Now going to commit!");
        if (!commit_outside_request) {
            conn.commit();
        }

        ((OracleConnection) conn).endRequest();
        if (commit_outside_request) {
            conn.commit();
        }

        conn.close();
        return true;
    }

}

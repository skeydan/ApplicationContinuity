package com.trivadis.ha.applicationContinuity;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.*;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class UCPTransactionProcessor extends AbstractTransactionProcessor {

    private static final String poolName = "application continuity pool";
    private static final PoolDataSource pds;

    private final static Logger logger = Logger.getLogger(UCPTransactionProcessor.class.getName());

    static {
        pds = PoolDataSourceFactory.getPoolDataSource();
        try {
            pds.setConnectionFactoryClassName("oracle.jdbc.replay.OracleDataSourceImpl");
            pds.setConnectionPoolName(poolName);
            pds.setMinPoolSize(2);
            pds.setMaxPoolSize(10);
            pds.setInitialPoolSize(5);
            pds.setFastConnectionFailoverEnabled(true);
        } catch (SQLException e) {
            logger.severe(e.getMessage());
        }
    }

    protected UCPTransactionProcessor(String url, String user, String passwd) throws IOException {

        getProperties();
        logger.info(pds.toString());
        try {
            pds.setUser(user);
            pds.setPassword(passwd);
            pds.setURL(url);
        } catch (SQLException ex) {
            Logger.getLogger(UCPTransactionProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    protected Connection getConnection() throws SQLException {

        Connection conn = null;
        while (conn == null) {
            try {
                conn = pds.getConnection();
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

        conn.setAutoCommit(false);
        DemoHelper.logConnectionPoolStats(pds);
        DemoHelper.logConnectedSessions(getConnection(), serviceName);
        DemoHelper.logSessionToKill(getConnection(), serviceName, appUser);
        logger.fine("Starting transaction.");

        DemoHelper.ORA_41412_sysdate(conn);
        if (ORA_41412_session) {
            DemoHelper.ORA_41412_session(conn);
        }

        transaction.execute(conn);

        logger.info("Work done. Now going to commit!");
        conn.commit();
        conn.close();
        return true;
    }
}

package com.trivadis.ha.applicationContinuity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import oracle.ucp.jdbc.JDBCConnectionPoolStatistics;
import oracle.ucp.jdbc.PoolDataSource;

public class DemoHelper {

    private final static Logger logger = Logger.getLogger(DemoHelper.class.getName());

    protected static void logSessionToKill(Connection conn, String serviceName, String userName) throws SQLException {

        conn.setAutoCommit(false);
        conn.setClientInfo("OCSID.ACTION", "logSessionToKill");

        PreparedStatement stmt = conn.prepareStatement("select sid, serial#, inst_id"
                + " from gv$session where service_name=? and action is null and username=?");
        stmt.setString(1, serviceName);
        stmt.setString(2, userName);

        ResultSet rset = stmt.executeQuery();
        StringBuilder killMe = new StringBuilder("Kill session: \nalter system kill session '");
        while (rset.next()) {
            int sid = rset.getInt("sid");
            int serial = rset.getInt("serial#");
            int instId = rset.getInt("inst_id");
            killMe.append(sid).append(",").append(serial).
                    append(",@").append(instId).append("'");

        }
        logger.info(killMe.toString());
        rset.close();
        stmt.close();
        conn.close();

    }

    protected static void logConnectedSessions(Connection conn, String serviceName) throws SQLException {

        conn.setAutoCommit(false);
        conn.setClientInfo("OCSID.ACTION", "logConnectedSessions");

        PreparedStatement stmt = conn.prepareStatement("select sid, serial#, inst_id, module, action, username, program"
                + " from gv$session where service_name=? ");
        stmt.setString(1, serviceName);

        ResultSet rset = stmt.executeQuery();
        StringBuilder sessions = new StringBuilder("Connected sessions: \n");
        while (rset.next()) {
            int sid = rset.getInt("sid");
            int serial = rset.getInt("serial#");
            int instId = rset.getInt("inst_id");
            String module = rset.getString("module");
            String action = rset.getString("action");
            String program = rset.getString("program");
            String username = rset.getString("username");
            sessions.append(lpad(String.valueOf(sid),3)).append(",").append((lpad(String.valueOf(serial),5))).append(",@").append(instId).append(lpad(module, 26)).
                    append(lpad(action, 36)).append(lpad(username, 10)).append(lpad(program, 42)).append("\n");
        }
        logger.info(sessions.toString());
        rset.close();
        stmt.close();

    }

    protected static void logConnectionPoolStats(PoolDataSource pds) {
        JDBCConnectionPoolStatistics stats = pds.getStatistics();
        if (stats != null) {
            StringBuilder sessions = new StringBuilder("Connection pool stats: \n");
            sessions.append("Total connections: ").append(stats.getTotalConnectionsCount()).append("\n");
            sessions.append("Borrowed connections: ").append(stats.getBorrowedConnectionsCount()).append("\n");
            sessions.append("Available connections: ").append(stats.getAvailableConnectionsCount());
            logger.info(sessions.toString());
        }
    }

    protected static void ORA_41412_sysdate(Connection conn) throws SQLException {
        
        ResultSet rset = conn.createStatement().executeQuery("select sysdate from dual");
        if (rset.next()) {
            logger.fine("select sysdate from dual" + rset.getDate(1));
        }
    }
    
    protected static void ORA_41412_session(Connection conn) throws SQLException {

        ResultSet rset = conn.createStatement().executeQuery("select avg(serial#) "
                + " from gv$session where service_name='pdb1_appcont'");
        if (rset.next()) {
            logger.fine("select sum(sid) from gv$session: " + rset.getInt(1));
        }

    }
    
    private static String lpad(String s, int n) {
    return String.format("%1$" + n + "s", s);
  }

    
}

package com.trivadis.ha.applicationContinuity;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public abstract class AbstractTransactionProcessor implements TransactionProcessor {

    Properties props = new Properties();
    protected String moduleName;
    protected String serviceName;
    protected String appUser;

    protected boolean ORA_41412_session;
    protected boolean session_state_changed;
    protected boolean ORA_14901;
    protected boolean commit_outside_request;
    protected boolean autocommit_outside_request;

    protected void getProperties() throws IOException {

        InputStream inputStream = Optional.ofNullable(getClass().getClassLoader().
                getResourceAsStream("config.properties")).orElseThrow(FileNotFoundException::new);
        props.load(inputStream);
        serviceName = props.getProperty("serviceName");
        appUser = props.getProperty("appUser");
        moduleName = props.getProperty("moduleName");
        ORA_41412_session = Boolean.parseBoolean(props.getProperty("ORA_41412_session"));
        ORA_14901 = Boolean.parseBoolean(props.getProperty("ORA_14901"));
        commit_outside_request = Boolean.parseBoolean(props.getProperty("commit_outside_request"));
        autocommit_outside_request = Boolean.parseBoolean(props.getProperty("autocommit_outside_request"));
        inputStream.close();
    }

    protected abstract Connection getConnection() throws SQLException;

}

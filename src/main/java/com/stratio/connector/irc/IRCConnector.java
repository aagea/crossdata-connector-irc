/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Stratio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.stratio.connector.irc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.stratio.connector.irc.engine.IRCMetadataEngine;
import com.stratio.connector.irc.engine.IRCQueryEngine;
import com.stratio.connector.irc.engine.IRCStorageEngine;
import com.stratio.connector.irc.manager.IRCManager;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.connector.IConnector;
import com.stratio.crossdata.common.connector.IMetadataEngine;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IStorageEngine;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.InitializationException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.security.ICredentials;
import com.stratio.crossdata.connectors.ConnectorApp;

/**
 * Connector main class that launches the connector actor wrapper and implements the
 * {@link com.stratio.crossdata.common.connector.IConnector} interface.
 */
public class IRCConnector implements IConnector{

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(IRCConnector.class);

    @Override
    public String getConnectorName() {
        return "IRCConnector";
    }

    @Override
    public String[] getDatastoreName() {
        return new String[]{"IRCDatastore"};
    }

    private Map<ClusterName, IRCManager> managers=new HashMap<>();

    @Override
    public void init(IConfiguration configuration) throws InitializationException {
        LOG.info("IRCConnector is INIT!");
    }
    private final static Object LOCK=new Object();
    @Override public void connect(ICredentials credentials, ConnectorClusterConfig config) throws ConnectionException {
        try {
            synchronized (LOCK) {
                if (!managers.containsKey(config.getName())) {
                    String host = "127.0.0.1";
                    if (config.getClusterOptions() != null && config.getClusterOptions().containsKey("host")) {
                        host = config.getClusterOptions().get("host");
                    }
                    String login = "xdbot";
                    if (config.getConnectorOptions() != null && config.getConnectorOptions().containsKey("name")) {
                        login = config.getConnectorOptions().get("name");
                    }
                    LOG.info("host: " + host + " login: " + login);
                    IRCManager manager = new IRCManager(host, login);
                    try {
                        manager.connect();
                        managers.put(config.getName(), manager);
                    } catch (Exception e) {
                        throw new ConnectionException(e);
                    }
                }
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override public void close(ClusterName name) throws ConnectionException {
        LOG.info("Close connection " + name.toString());
        IRCManager manager=managers.get(name);
        manager.disconnect();
    }

    @Override public void shutdown() throws ExecutionException {
        LOG.info("Shutdown connector!");
    }

    @Override public boolean isConnected(ClusterName name) {
        return managers.containsKey(name);
    }

    @Override
    public IStorageEngine getStorageEngine() throws UnsupportedException {
        return new IRCStorageEngine(managers);
    }

    @Override
    public IQueryEngine getQueryEngine() throws UnsupportedException {
        return new IRCQueryEngine(managers);
    }

    @Override
    public IMetadataEngine getMetadataEngine() throws UnsupportedException {
        return new IRCMetadataEngine(managers);
    }

    /**
     * Run a Skeleton Connector using a {@link com.stratio.crossdata.connectors.ConnectorApp}.
     * @param args The arguments.
     */
    public static void main(String [] args){
        IRCConnector ircConnector = new IRCConnector();
        ConnectorApp connectorApp = new ConnectorApp();
        connectorApp.startup(ircConnector);
    }

}

package com.reinvent.synergy.data.tunnel;

import com.reinvent.synergy.data.model.*;
import com.reinvent.synergy.data.primarykey.AbstractPrimaryKey;
import com.reinvent.synergy.data.primarykey.IntegerPrimaryKey;
import com.reinvent.synergy.data.system.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Bohdan Mushkevych
 * Description: Tunnel Server opens TCP port and parses every input as JSON object that is inserted to Hourly
 * collections in HBase
 */

public class TunnelServer<T> extends Thread {
    public static final String PROPERTY_LOG4J = "log4j.configuration";
    public static final String PROPERTY_TUNNEL = "tunnel.properties";

    protected boolean isThreadRunning = true;
    protected int port;

    protected Logger logger;
    protected PoolManager<T> poolManager;
    protected Class<T> clazzDataModel;
    protected AbstractPrimaryKey primaryKey;
    protected ExecutorService threadPool = Executors.newCachedThreadPool();

    static {
        PropertyConfigurator.configure(System.getProperty(PROPERTY_LOG4J));
    }

    public TunnelServer(int port, Class<T> clazzDataModel, AbstractPrimaryKey primaryKey, String tableName) {
        this.port = port;
        this.clazzDataModel = clazzDataModel;
        this.poolManager = new PoolManager<T>(tableName, clazzDataModel, primaryKey);
        logger = Logger.getLogger(this.clazzDataModel.getSimpleName());
        logger.info(String.format("Started Synergy Tunnel on %s for %s", this.port, this.clazzDataModel.getName()));
    }

    public void run() {
        try {
            ServerSocket welcomeSocket = new ServerSocket(port);

            while(isThreadRunning)
            {
                try {
                    Socket connectionSocket = welcomeSocket.accept();
                    if (connectionSocket != null) {
                        threadPool.submit(new TunnelWorker<T>(connectionSocket, clazzDataModel, poolManager));
                    } else {
                        logger.info("Accepted socket is null. Exiting main thread.");
                    }
                } catch (Exception e) {
                    logger.error("Exception on worker/socket level", e);
                }
            }
        } catch (IOException e) {
            logger.error("Server side exception.", e);
        }
    }

    public void stopTunnelServer() {
        isThreadRunning = false;
    }

    public static void main(String argv[]) throws Exception
    {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTY_TUNNEL));
        } catch (IOException e) {
            Logger.getRootLogger().error("Can not read tunnel.properties. Applying default values.", e);
        }

        int port = Integer.parseInt(properties.getProperty("tunnel.example.port", "9999"));
        TunnelServer<Example> serverExample = new TunnelServer<Example>(
                port, Example.class, new IntegerPrimaryKey(), Constants.TABLE_EXAMPLE);

        serverExample.start();
    }
}

package com.reinvent.synergy.data.tunnel;

import com.reinvent.synergy.data.mapping.EntityService;
import com.reinvent.synergy.data.mapping.JsonService;
import com.reinvent.synergy.data.system.PoolManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @author Bohdan Mushkevych
 * date: 16/09/11
 * Description: Module reads serialized stream from Python Hourly aggregators and converts it to the Synergy Data models
 * Afterwards - converted objects are inserted into HBase via Synergy ORM
 */
public class TunnelWorker<T> implements Runnable {
    protected Logger logger;
    protected Socket connectionSocket;
    protected BufferedReader inFromClient;
    protected DataOutputStream outToClient;
    protected Class<T> clazz;
    protected PoolManager<T> poolManager;

    public static final int STATUS_OK = 0;
    public static final int STATUS_NOT_OK = 999;
    public static final String COMMAND_FLUSH = "FLUSH";


    public TunnelWorker(Socket connectionSocket,
                        Class<T> clazzDataModel,
                        PoolManager<T> poolManager) throws IOException {
        this.clazz = clazzDataModel;
        this.connectionSocket = connectionSocket;
        this.poolManager = poolManager;
        this.logger = Logger.getLogger(this.clazz.getSimpleName());

        inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
        outToClient = new DataOutputStream(connectionSocket.getOutputStream());
    }

    public void run() {
        boolean isOK = true;
        HTable table = null;
        EntityService<T> entityService = null;
        JsonService<T> jsonService = null;
        String jsonObj = null;

        try {
            jsonObj = inFromClient.readLine();
        } catch (IOException e) {
            isOK = false;
            logger.error("Error on reading the socket", e);
        }

        try {
            if (isOK) {
                table = poolManager.getTable();
                entityService = poolManager.getEntityService();
                jsonService = poolManager.getJsonService();

                if (COMMAND_FLUSH.equals(jsonObj)) {
                    poolManager.flushTable();
                } else {
                    T obj = jsonService.fromJson(jsonObj);
                    Put p = entityService.insert(obj);
                    table.put(p);
                }
            }
        } catch (IOException e) {
            isOK = false;
            logger.error("Error on HBase level", e);
        } catch (Exception e) {
            isOK = false;
            logger.error(String.format("Unexpected exception on processing JSON: %s", jsonObj), e);
        } finally {
            if (table != null) {
                poolManager.putTable(table);
            }
            if (entityService != null) {
                poolManager.putEntityService(entityService);
            }
            if (jsonService != null) {
                poolManager.putJsonService(jsonService);
            }
        }

        try {
            if (!poolManager.isFlushRequested()) {
                // there is no sense in talking to socket if the client has gone already
                if (isOK) {
                    outToClient.write(STATUS_OK);
                } else {
                    outToClient.write(STATUS_NOT_OK);
                }
            }
        } catch (IOException e) {
            logger.error("Error on talking back to socket", e);
        } finally {
            try {
                connectionSocket.close();
            } catch (IOException e) {
                logger.error("Exception on closing socket", e);
            }
        }
    }
}

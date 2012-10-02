package com.reinvent.surus.system;

import com.reinvent.surus.mapping.EntityService;
import com.reinvent.surus.mapping.JsonService;
import com.reinvent.surus.primarykey.AbstractPrimaryKey;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 * @author Bohdan Mushkevych
 * Description: Thread-safe pool manager of re-usable resources for Tunnel Workers
 */
public class PoolManager<T> {
    protected static final int POOL_SIZE = 128;
    protected static final long BUFFER_SIZE = 1024*2048; // 2 MB

    private final Object lockJson = new Object();
    private final Object lockTable = new Object();
    private final Object lockEntity = new Object();

    protected int connectionCounter = 0;
    protected Logger log;

    protected String tableName;
    protected Class<T> clazzDataModel;
    protected AbstractPrimaryKey primaryKey;

    protected Deque<EntityService<T>> dequeEntityService = new ArrayDeque<EntityService<T>>(POOL_SIZE);
    protected Deque<JsonService<T>> dequeJsonService = new ArrayDeque<JsonService<T>>(POOL_SIZE);
    protected HTablePool poolTable;

    public PoolManager(String tableName, Class<T> clazzDataModel, AbstractPrimaryKey primaryKey) {
        this.tableName = tableName;
        this.clazzDataModel = clazzDataModel;
        this.primaryKey = primaryKey;
        this.poolTable = new HTablePool(HBaseConfiguration.create(), POOL_SIZE);
        log = Logger.getLogger(tableName);
        for (int i = 0; i < POOL_SIZE; i++) {
            dequeEntityService.add(new EntityService<T>(clazzDataModel));
            dequeJsonService.add(new JsonService<T>(clazzDataModel, primaryKey));
        }
    }

    public EntityService<T> getEntityService() {
        synchronized (lockEntity) {
            try {
                return dequeEntityService.pop();
            } catch (NoSuchElementException e) {
                return new EntityService<T>(this.clazzDataModel);
            }
        }
    }

    public void putEntityService(EntityService<T> entityService) {
        synchronized (lockEntity) {
            dequeEntityService.push(entityService);
        }
    }

    public JsonService<T> getJsonService() {
        synchronized (lockJson) {
            try {
                return dequeJsonService.pop();
            } catch (NoSuchElementException e) {
                return new JsonService<T>(this.clazzDataModel, this.primaryKey);
            }
        }
    }

    public void putJsonService(JsonService<T> jsonService) {
        synchronized (lockJson) {
            dequeJsonService.push(jsonService);
        }
    }

    public AbstractPrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * from the HBase classes:
     * HTableInterface.close() rather than returning the tables to the pool
     */
    public void putTable(HTableInterface table) {
        synchronized (lockTable) {
            connectionCounter--;

            try {
                table.close();
            } catch (IOException e1) {
                log.error("Error on explicit HTable closure.", e1);
            }
        }
    }

    public HTable getTable() throws IOException {
        synchronized (lockTable) {
            connectionCounter++;
            HTable table = (HTable) poolTable.getTable(tableName);
            table.setAutoFlush(false);
            table.setWriteBufferSize(BUFFER_SIZE);
            return table;
        }
    }
}

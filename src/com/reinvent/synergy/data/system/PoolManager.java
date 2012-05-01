package com.reinvent.synergy.data.system;

import com.reinvent.synergy.data.mapping.EntityService;
import com.reinvent.synergy.data.mapping.JsonService;
import com.reinvent.synergy.data.primarykey.AbstractPrimaryKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 * @author Bohdan Mushkevych
 * date: 16/09/11
 * Description: Thread-safe pool manager of re-usable resources for Tunnel Workers
 */
public class PoolManager<T> {
    protected static final int FLUSH_WINDOW_MILLIS = 30000; //30 seconds
    protected static final int POOL_SIZE = 128;
    protected static final long BUFFER_SIZE = 1024*2048; // 2 MB

    private final Object lockJson = new Object();
    private final Object lockTable = new Object();
    private final Object lockEntity = new Object();

    protected boolean isFlushRequested = false;
    protected long flushTimeMillis = 0;
    protected int connectionCounter = 0;
    protected Logger log;

    protected String tableName;
    protected Class<T> clazzDataModel;
    protected AbstractPrimaryKey primaryKey;

    protected Deque<EntityService<T>> dequeEntityService = new ArrayDeque<EntityService<T>>(POOL_SIZE);
    protected Deque<JsonService<T>> dequeJsonService = new ArrayDeque<JsonService<T>>(POOL_SIZE);
    protected HTablePool poolTable = new HTablePool();

    public PoolManager(String tableName, Class<T> clazzDataModel, AbstractPrimaryKey primaryKey) {
        this.tableName = tableName;
        this.clazzDataModel = clazzDataModel;
        this.primaryKey = primaryKey;
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

    public boolean isFlushRequested() {
        return isFlushRequested;
    }

    public AbstractPrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void putTable(HTable table) {
        synchronized (lockTable) {
            connectionCounter--;
            poolTable.putTable(table);

            if (isFlushRequested) {
                flushTable();
            }
        }
    }

    public HTable getTable() throws IOException {
        synchronized (lockTable) {
            if (isFlushRequested) {
                flushTable();
            }

            connectionCounter++;
            HTable table = (HTable) poolTable.getTable(tableName);
            table.setAutoFlush(false);
            table.setWriteBufferSize(BUFFER_SIZE);
            return table;
        }
    }

    public void flushTable() {
        synchronized (lockTable) {
            if (!isFlushRequested) {
                flushTimeMillis = System.currentTimeMillis();
                isFlushRequested = true;
            }
            if (System.currentTimeMillis() - flushTimeMillis > FLUSH_WINDOW_MILLIS) {
                log.warn(String.format("Closing HBase pool due to time-out. Connection delta = %s", connectionCounter));
                connectionCounter = 0;
            }
            if (connectionCounter == 0) {
                poolTable.closeTablePool(tableName);
                poolTable = new HTablePool();
                flushTimeMillis = 0;
                isFlushRequested = false;
            }
        }
    }
}

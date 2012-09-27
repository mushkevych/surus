package com.reinvent.synergy.data.rest;

import com.reinvent.synergy.data.model.Constants;
import com.reinvent.synergy.data.system.PoolManager;
import com.reinvent.synergy.data.system.TableContext;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Bohdan Mushkevych
 * Description: handles put/post requests from UI
 */
public class ExamplePostHandler implements RequestHandler {
    protected Logger logger = Logger.getLogger(ExamplePostHandler.class.getSimpleName());

    protected HttpServletRequest request;
    protected HttpServletResponse response;

    protected PoolManager pmExample = TableContext.getPoolManager(Constants.TABLE_EXAMPLE);

    protected Integer domainId;
    protected Integer keywordId;
    protected String value;
    protected boolean isValid;


    public ExamplePostHandler(HttpServletRequest req, HttpServletResponse resp) {
        request = req;
        response = resp;

        try {
            domainId = Integer.valueOf(request.getParameter(PARAMETER_DOMAIN_ID));
            keywordId = Integer.valueOf(request.getParameter(PARAMETER_KEYWORD_ID));
            value = request.getParameter(PARAMETER_KEYWORD_VALUE);
            isValid = true;
        } catch (Exception e) {
            logger.warn(String.format("Request is considered invalid (domain, keyword, value): (%s, %s, %s)",
                    request.getParameter(PARAMETER_DOMAIN_ID),
                    request.getParameter(PARAMETER_KEYWORD_ID),
                    request.getParameter(PARAMETER_KEYWORD_VALUE)), e);
            isValid = false;
        }
    }

    public void run() {
        if (!isValid) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        HTable tExample = null;

        try {
            tExample = pmExample.getTable();

            Put put = new Put(Bytes.toBytes(domainId));
            put.add(Bytes.toBytes(Constants.FAMILY_KEYWORD), Bytes.toBytes(keywordId), Bytes.toBytes(value));

            tExample.put(put);
            tExample.flushCommits();
            response.setStatus(HttpServletResponse.SC_OK);
        } catch (IOException e) {
            logger.warn(String.format("Exception on inserting Example record (domain, keyword): (%s, %s)", domainId, keywordId), e);
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            logger.warn(String.format("Exception on inserting Example record (domain, keyword): (%s, %s)", domainId, keywordId), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            if (tExample != null) {
                pmExample.putTable(tExample);
            }
        }
    }
}

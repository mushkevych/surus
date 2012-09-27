package com.reinvent.synergy.data.rest;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Bohdan Mushkevych
 * Description: REST server for the Surus project
 */
public class RestServer {
    public static final String PROPERTY_LOG4J = "log4j.configuration";
    protected static Logger log;
    static {
        System.setProperty("slf4j", "false");
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Log4JLogger");
        PropertyConfigurator.configure(System.getProperty(PROPERTY_LOG4J));
        log = Logger.getLogger(RestServer.class.getSimpleName());
    }

    public static void main(String[] args) {
        try {
            Server server = new Server(11111);

            ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            context.setContextPath("/");

            context.addServlet(new ServletHolder(new HttpServlet() {
                @Override
                protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                    new ExamplePostHandler(req, resp).run();
                }
            } ), "/add_example_entry/*");

            context.addServlet(new ServletHolder(new HttpServlet() {
                @Override
                protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                    new ExampleDeleteHandler(req, resp).run();
                }
            } ), "/delete_example_entry/*");

            log.info("Surus REST initialized. Main loop starting...");

            server.setHandler(context);
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.hiido.hcat.databus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zrc on 16-12-14.
 */
public class ServiceMain {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

    private static String SERVER_BEAN_ID = "databusserver";

    public static void main(String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("databus.xml");
        HcatDatabusServer server = ctx.getBean(SERVER_BEAN_ID, HcatDatabusServer.class);
        try {
            server.start();
            LOG.info("Databus is started.");
            Thread.currentThread().join();
        } catch (Exception e) {
            LOG.error("Databus Server stop with exception.", e);
        } finally {
            LOG.info("Databus Server is closed now.");
        }
    }
}

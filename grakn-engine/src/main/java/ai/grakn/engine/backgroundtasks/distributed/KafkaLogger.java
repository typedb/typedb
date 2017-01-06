/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.backgroundtasks.distributed;

import ai.grakn.engine.backgroundtasks.config.ConfigHelper;
import ai.grakn.engine.util.ConfigProperties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.grakn.engine.backgroundtasks.config.KafkaTerms.LOG_TOPIC;

public class KafkaLogger {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaLogger.class);
    private final LogLevel logLevel;
    private static KafkaLogger instance = null;
    private KafkaProducer<String, String> producer;

    private enum LogLevel {
        DEBUG("DEBUG", 0),
        INFO("INFO", 1),
        WARN("WARN", 2),
        ERROR("ERROR", 3);

        private int level;
        private String value;
        LogLevel(String value, int level) {
            this.value = value;
            this.level = level;
        }

        public String toString() {
            return value;
        }

        public int level() {
            return level;
        }
    }

    public static synchronized KafkaLogger getInstance() {
        if(instance == null)
            instance = new KafkaLogger();

        return instance;
    }

    public void debug(String msg) {
        if(logLevel.level() <= LogLevel.DEBUG.level())
            sendMsg(LogLevel.DEBUG.toString(), Thread.currentThread().getStackTrace()[2].toString(), msg);
        LOG.debug(msg);
    }

    public void info(String msg) {
        if(logLevel.level() <= LogLevel.INFO.level())
            sendMsg(LogLevel.INFO.toString(), Thread.currentThread().getStackTrace()[2].toString(), msg);
        LOG.info(msg);
    }

    public void warn(String msg) {
        if(logLevel.level() <= LogLevel.WARN.level())
        sendMsg(LogLevel.WARN.toString(), Thread.currentThread().getStackTrace()[2].toString(), msg);
        LOG.warn(msg);
    }

    public void error(String msg) {
        if(logLevel.level() <= LogLevel.ERROR.level())
        sendMsg(LogLevel.ERROR.toString(), Thread.currentThread().getStackTrace()[2].toString(), msg);
        LOG.error(msg);
    }
    
    public void error(String msg, Throwable ex) {
        if(logLevel.level() <= LogLevel.ERROR.level())
        sendMsg(LogLevel.ERROR.toString(), 
        		Thread.currentThread().getStackTrace()[2].toString(), 
        		msg + "\n" + ExceptionUtils.getFullStackTrace(ex));
        LOG.error(msg);
    }

    void open() {
//        producer = ConfigHelper.kafkaProducer();
    }

    void close() {
//        producer.flush();
//        producer.close();
    }

    private KafkaLogger() {
        logLevel = LogLevel.DEBUG;//LogLevel.valueOf(ConfigProperties.getInstance().getProperty(ConfigProperties.LOGGING_LEVEL));
    }

    private void sendMsg(String level, String caller, String msg) {
    	System.out.println("LOG from " + caller + ": " + msg);
//        ProducerRecord record = new ProducerRecord(LOG_TOPIC, level + " - " + caller + " - " + msg);
//        producer.send(record);
    }
}

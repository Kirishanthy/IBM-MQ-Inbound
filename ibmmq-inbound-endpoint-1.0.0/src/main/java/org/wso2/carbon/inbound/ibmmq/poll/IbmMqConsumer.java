/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.ibmmq.poll;

import com.ibm.mq.MQQueueManager;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.constants.MQConstants;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;

import java.io.IOException;
import java.util.Properties;

public class IbmMqConsumer extends GenericPollingConsumer {
    private static final Log log = LogFactory.getLog(IbmMqConsumer.class);
    MQQueueManager queueManager;
    private MQQueue queue;
    private boolean isConnected;

    public IbmMqConsumer(Properties ibmMqProperties, String name, SynapseEnvironment synapseEnvironment,
                         long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                         boolean sequential) {
        super(ibmMqProperties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                sequential);
        log.info("Initialized the IBM MQ consumer");
        setupConnection();
    }

    public Object poll() {
        if (!isConnected) {
            setupConnection();
        }
        if (isConnected) {
            messageFromQueue();
        }
        return null;
    }

    /**
     * Injecting the IBM MQ to the sequence
     *
     * @param message the IBM MQ response message
     */
    public void injectIbmMqMessage(String message) {
        if (injectingSeq != null) {
            String content = properties.getProperty(ibmMqConstant.CONTENT_TYPE);
            injectMessage(message, content);
            if (log.isDebugEnabled()) {
                log.debug("Injecting IBM MQ  message to the sequence : " + injectingSeq);
            }
        } else {
            handleException("The Sequence is not found");
        }
    }

    /**
     * Setting up the connection
     */
    private void setupConnection() {
        if (log.isDebugEnabled()) {
            log.debug("Starting to setup the connection with the IBM MQ");
        }
        String hostname = properties.getProperty(ibmMqConstant.HOST_NAME);
        String channel = properties.getProperty(ibmMqConstant.CHANNEL);
        String qName = properties.getProperty(ibmMqConstant.MQ_QUEUE);
        String qManager = properties.getProperty(ibmMqConstant.MQ_QMGR);
        String port = properties.getProperty(ibmMqConstant.PORT);
        String userName = properties.getProperty(ibmMqConstant.USER_ID);
        String password = properties.getProperty(ibmMqConstant.PASSWORD);
        String sslEnabledS = properties.getProperty(ibmMqConstant.SSL_ENABLED);
        boolean sslEnabled = Boolean.parseBoolean(sslEnabledS);
        MQEnvironment.hostname = hostname;
        MQEnvironment.channel = channel;
        MQEnvironment.userID = userName;
        MQEnvironment.password = password;
        MQEnvironment.port = Integer.parseInt(port);
        if (sslEnabled) {
            sslConnection();
        }
        try {
            queueManager = new MQQueueManager(qManager);
            int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF;
            queue = queueManager.accessQueue(qName, openOptions);
            log.info("IBM MQ " + qManager + " queue manager successfully connected");
            isConnected = true;
        } catch (MQException e) {
            handleException("Error while setup the IBM MQ connection", e);
            isConnected = false;
        }
    }

    /**
     * Receiving message from queue
     */
    private void messageFromQueue() {
        try {
            MQMessage rcvMessage = new MQMessage();
            MQGetMessageOptions gmo = new MQGetMessageOptions();
            queue.get(rcvMessage, gmo);
            String msgText = rcvMessage.readUTF();
            log.info("Channel :" + MQEnvironment.channel);
            injectIbmMqMessage(msgText);
        } catch (MQException e) {
            int reason = e.reasonCode;
            if (reason == 2009) {
                isConnected = false;
                log.error("IBM MQ Connection Broken");
            } else if (reason != 2033) {
                log.error("Error while getting messages from queue", e);
            }
        } catch (IOException e) {
            handleException(e.getMessage());
        }
    }

    /*
     * SSL connection
     */
    public void sslConnection() {
        String keyStoreLocation = properties.getProperty(ibmMqConstant.SSL_KEYSTORE_LOCATION);
        String keyStoreType = properties.getProperty(ibmMqConstant.SSL_KEYSTORE_TYPE);
        String keyStorePassword = properties.getProperty(ibmMqConstant.SSL_KEYSTORE_PASSWORD);
        String trustStoreLocation = properties.getProperty(ibmMqConstant.SSL_TRUSTSTORE_LOCATION);
        String trustStoreType = properties.getProperty(ibmMqConstant.SSL_TRUSTSTORE_TYPE);
        String sslVersion = properties.getProperty(ibmMqConstant.SSL_VERSION);
        String sslFipsRequired = properties.getProperty(ibmMqConstant.SSL_FIPS);
        String sslCipherSuite = properties.getProperty(ibmMqConstant.SSL_CIPHERSUITE);
        boolean sslFips = Boolean.parseBoolean(sslFipsRequired);
        try {
            char[] keyPassphrase = keyStorePassword.toCharArray();
            KeyStore ks = KeyStore.getInstance(keyStoreType);
            ks.load(new FileInputStream(keyStoreLocation), keyPassphrase);
            KeyStore trustStore = KeyStore.getInstance(trustStoreType);
            trustStore.load(new FileInputStream(trustStoreLocation), null);

            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            trustManagerFactory.init(trustStore);
            keyManagerFactory.init(ks, keyPassphrase);
            SSLContext sslContext = SSLContext.getInstance(sslVersion);
            sslContext.init(keyManagerFactory.getKeyManagers(),
                    trustManagerFactory.getTrustManagers(), null);
            MQEnvironment.sslSocketFactory = sslContext.getSocketFactory();
            MQEnvironment.sslFipsRequired = sslFips;
            MQEnvironment.sslCipherSuite = sslCipherSuite;
        } catch (Exception ex) {
            handleException(ex.getMessage());
        }
    }

    private void handleException(String msg, Exception ex) {
        log.error(msg, ex);
        throw new SynapseException(ex);
    }

    private void handleException(String msg) {
        log.error(msg);
        throw new SynapseException(msg);
    }
}

package com.crowdfund.mq;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

public class ProducerMq 
{
    private final Producer<String, String> producer;
    
    private Logger logger = Logger.getLogger(this.getClass());
    
    /**
     * 构造方法  初始化参数和producer对象
     */
    private ProducerMq(){
        producer = new Producer<String, String>(buildProducerConfig());
    }

    public void produce(String topic,String data,String key) {
    	try {
    		producer.send(new KeyedMessage<String, String>(topic, key ,data));
    		logger.info("--------produce data --------------" + data);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
        
        
    }

    public ProducerConfig buildProducerConfig(){
    	Properties props = new Properties();
        //此处配置的是kafka的端口
        //配置value的序列化类
        try {
        	props.put("serializer.class", "kafka.serializer.StringEncoder");
            //配置key的序列化类
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            
            props.put("metadata.broker.list", ReadProperty.
            		getPropertiesByName("config.properties", "metadata.broker.list"));
            
            props.put("zookeeper.connect", ReadProperty.
            		getPropertiesByName("config.properties", "zk.server"));//声明zk
            //消息同步发送
            props.put("producer.type", ReadProperty.
            		getPropertiesByName("config.properties", "producer.type"));
            //当master接收到信息即发送到ack
            props.put("request.required.acks", ReadProperty.
            		getPropertiesByName("config.properties", "request.required.acks"));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("---------- buildProducerConfig error ----------------" + e);
		}
        return new ProducerConfig(props);
    }
}

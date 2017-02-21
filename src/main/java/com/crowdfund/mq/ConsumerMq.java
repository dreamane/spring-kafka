package com.crowdfund.mq;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.support.ApplicationObjectSupport;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.WebApplicationContext;

public class ConsumerMq implements ApplicationListener<ContextRefreshedEvent>{
	
	private final ConsumerConnector consumer;
	
    private ExecutorService executor;
    
    WebApplicationContext wac = null;
    
    private Logger logger = Logger.getLogger(this.getClass());
    
    public ConsumerMq() {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
    }
 
    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
    }
 
    public void runConsumer() {
    	try {
    		int numThreads = Integer.valueOf(ReadProperty.
            		getPropertiesByName("config.properties", "num.threads"));
        	String topic = ReadProperty.
            		getPropertiesByName("config.properties", "topic");
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(topic, new Integer(numThreads));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                    .createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            executor = Executors.newFixedThreadPool(numThreads);
            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                executor.submit(new ConsumerMsgTask(stream, threadNumber));
                threadNumber++;
            }
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("createConsumerConfig error:" + e);
		}
    	
    }

    private ConsumerConfig createConsumerConfig() {
    	Properties props = new Properties();
        try {
        	//zookeeper 配置
            props.put("zookeeper.connect", ReadProperty.
            		getPropertiesByName("config.properties", "zk.server"));
            //group 代表一个消费组
            props.put("group.id", ReadProperty.
            		getPropertiesByName("config.properties", "group.id"));
            //zk连接超时
            props.put("zookeeper.session.timeout.ms", ReadProperty.
            		getPropertiesByName("config.properties", "zookeeper.session.timeout.ms"));
            props.put("zookeeper.sync.time.ms", ReadProperty.
            		getPropertiesByName("config.properties", "zookeeper.sync.time.ms"));
            props.put("auto.commit.interval.ms", ReadProperty.
            		getPropertiesByName("config.properties", "auto.commit.interval.ms"));
            props.put("auto.offset.reset", ReadProperty.
            		getPropertiesByName("config.properties", "auto.offset.reset"));
            //序列化类
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("createConsumerConfig error:" + e);
		}
        return new ConsumerConfig(props);
    }
 
    /**
     * spring容器加载完毕后执行此方法  启动consumer
     */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		logger.info("-------------method in onApplicationEvent-------------------");
		ConsumerMq consumerMq = new ConsumerMq();
		consumerMq.runConsumer();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
        	e.printStackTrace();
        	logger.error("onApplicationEvent error:" + e);
        }
//        demo.shutdown();
	}
	
	public class ConsumerMsgTask extends ApplicationObjectSupport implements Runnable {

		private KafkaStream m_stream;
	    private int m_threadNumber;
	    private Logger logger = Logger.getLogger(this.getClass());

	    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
	        m_threadNumber = threadNumber;
	        m_stream = stream;
	    }

	    public void run() {
	    	try {
	    		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		        while (it.hasNext()){
		        	String recieveMsg = new String(it.next().message());
		        	logger.info(new Date().getTime() + " Thread " + m_threadNumber + ": " + recieveMsg);
		        	if(wac == null){
		        		wac = ContextLoader.getCurrentWebApplicationContext();
		        	}
		        	if(wac.getBean("consumerService") != null){
		        		ConsumerService consumerService = (ConsumerService)wac.getBean("consumerService");
			        	consumerService.reduceBiz(recieveMsg);
		        	}
		        }
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
			}
	    }
	    
	}
}
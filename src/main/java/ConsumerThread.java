import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class ConsumerThread implements  Runnable {
    private static final Logger log = LogManager.getLogger(Consumer.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;
    static float latency;
    static Double maxConsumptionRatePerConsumer1 = 0.0d;
    public static void main(String[] args) {
    }


    private static void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    this.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    private static void startServer() {
        Thread server = new Thread(new ServerThread());
        server.start();
    }

    @Override
    public void run() {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                StickyAssignor.class.getName());
    /*    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RangeAssignor.class.getName());
*/
/*
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BinPackPartitionAssignor.class.getName());*/
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()), new RebalanceListener());
        log.info("Subscribed to topic {}", config.getTopic());
        addShutDownHook();
        // PrometheusUtils.initPrometheus();
        //startServer();
        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));
                //max = 0;
                if (records.count() != 0) {
                    log.info("received {}", records.count());
                    for (ConsumerRecord<String, Customer> record : records) {
                        totalEvents++;
                        //TODO sleep per record or per batch
                        try {
                            Thread.sleep(Long.parseLong(config.getSleep()));
                            if (System.currentTimeMillis() - record.timestamp() <= 500 /*1500*/) {
                                eventsNonViolating++;
                            } else {
                                eventsViolating++;
                            }
                            log.info(" latency is {}", System.currentTimeMillis() - record.timestamp());
                            //function to do object detection...
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    consumer.commitSync();
                    double percentViolating = eventsViolating / totalEvents;
                    double percentNonViolating = eventsNonViolating / totalEvents;
                    log.info("Percent violating so far {}", percentViolating);
                    log.info("Events Non Violating {}", eventsNonViolating);
                    log.info("Total Events {}", totalEvents);
                    log.info("Percent non violating so far {}", percentNonViolating);
                    log.info("total events {}", totalEvents);
                }
            }

        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }

    }
}
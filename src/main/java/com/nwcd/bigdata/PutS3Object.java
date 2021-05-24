package com.nwcd.bigdata;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.opencsv.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PutS3Object {
    public static void main(String[] args) {
        String bucket_name = "nyc-tlc";
        String file_path = "D:\\workspace\\IdeaProjects\\Demo\\src\\main\\resources\\result.csv";
        String key_name = Paths.get(file_path).getFileName().toString();

        String s3_file_path = args[0];
        System.out.format("Uploading the result data in %s to S3 bucket %s...\n", s3_file_path, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();

//        try {
//            s3.putObject(bucket_name, key_name, new File(file_path));
//        } catch (AmazonServiceException e) {
//            System.err.println(e.getErrorMessage());
//            System.exit(1);
//        }

        //kafka参数声明
        String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        String topic_put = "put-s3data";
        String group = "bigdata";
        String deserialization = "org.apache.kafka.common.serialization.StringDeserializer";

        Properties properties=new Properties();
        properties.put("bootstrap.servers",brokers);
        properties.put("group.id", "bigdata");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", deserialization);
        properties.put("value.deserializer", deserialization);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic_put));
        int row_cnt = 0;
        String header = "vendorID,pickup_datetime,dropoff_datetime,trip_time,distance,average_speed,PULocationID,DOLocationID\r\n";

        //无限循环轮询
        try {
            long a = new Date().getTime();
            String ts = a+"";
//            FileWriter fw = new FileWriter("D:\\workspace\\IdeaProjects\\Demo\\src\\main\\resources\\nyctlc-etl-"+ts+".csv");
            FileWriter fw = new FileWriter(s3_file_path+"\\nyctlc-etl-"+ts+".csv");
            while (true) {
                /**
                 * 消费者必须持续对Kafka进行轮询，否则会被认为已经死亡，他的分区会被移交给群组里的其他消费者。
                 * poll返回一个记录列表，每个记录包含了记录所属主题的信息，
                 * 记录所在分区的信息，记录在分区里的偏移量，以及键值对。
                 * poll需要一个指定的超时参数，指定了方法在多久后可以返回。
                 * 发送心跳的频率，告诉群组协调器自己还活着。
                 */
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                ;
                for (ConsumerRecord<String, String> record : records) {
                    if (row_cnt == 0) {
                        fw.write(header);
                        row_cnt++;
                    }
                    else if (row_cnt == 99)
                    {
                        row_cnt = 0;
                    }
                    else
                    {
                        row_cnt++;
                    }
                    //Thread.sleep(1000);
//                    System.out.printf("offset = %d, value = %s", record.offset(), record.value());
//                    System.out.println();
                    fw.write(record.value()+"\r\n");
                    fw.flush();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

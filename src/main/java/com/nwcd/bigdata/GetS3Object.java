package com.nwcd.bigdata;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.*;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class GetS3Object {
    public static void main(String[] args) {
        // 设置S3桶名
        String bucket_name = "nyc-tlc";
        String key_name = "trip data/yellow_tripdata_2020-01.csv";
        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();

        // 创建生产者
        Properties kafkaProps = new Properties();
        // 指定broker（这里指定了2个，1个备用），如果你是集群更改主机名即可，如果不是只写运行的主机名
        kafkaProps.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        kafkaProps.put("group.id", "CountryCounter");	// 消费者群组
        // 设置序列化（自带的StringSerializer，如果消息的值为对象，就需要使用其他序列化方式，如Avro ）
        // key序列化
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 实例化出producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        String topic = "get-s3data";

        InputStreamReader fr = null;
        BufferedReader br = null;

        try {
            S3Object o = s3.getObject(bucket_name, key_name);
            //流式读取S3中文件
            S3ObjectInputStream s3is = o.getObjectContent();
            fr = new InputStreamReader(s3is, "UTF-8");
            br = new BufferedReader(fr);
            String rec = null;
            String[] argsArr = null;
            while ((rec = br.readLine()) != null) {
                if (rec != null) {
                    argsArr = rec.split("\n");  //csv的换行符\n
                    for (int i = 0; i < argsArr.length; i++) {
                        System.out.print("num " + (i + 1) + ":" + argsArr[i] + "\t");
                        producer.send(new ProducerRecord<String, String>(topic, argsArr[i]));
                        Thread.sleep(300);
                    }
                    System.out.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {
            producer.close();
            try {
                if (fr != null)
                    fr.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            try {
                if (br != null)
                    br.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

        System.out.println("Done!");
    }
}
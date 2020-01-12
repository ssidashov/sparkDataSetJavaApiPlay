package ru.open.test.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext run = SpringApplication.run(DemoApplication.class, args);

        Worker bean = run.getBean(Worker.class);
        bean.execute();
        Thread.sleep(100000);
    }

    @Bean
    public SparkContext sparkContext() {
        return new SparkContext(sparkConfig());
    }

    @Bean
    public SparkConf sparkConfig() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Demo");
        sparkConf.setMaster("local[*]");
        return sparkConf;
    }

}

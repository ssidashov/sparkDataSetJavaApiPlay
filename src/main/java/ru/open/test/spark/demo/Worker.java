package ru.open.test.spark.demo;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.STRING;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Function1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Service
public class Worker {

    @Autowired
    private SparkContext sparkContext;

    private Encoder<Person> personEncoder = Encoders.bean(Person.class);

    public void execute() {
        SparkSession sparkSession = new SparkSession(sparkContext);

        Dataset<Row> sourceCSV = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("1.csv")
                .withColumnRenamed("birthdate", "birthdateSource")
                .withColumn("company", typedLit(null, STRING.scalaTag()))
                .withColumn("birthdate", to_date(col("birthdateSource"), "yyyy.MM.dd"));

        Dataset<Company> companies = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("company.csv")
                .map(new CreateCompanyFunction(), Encoders.bean(Company.class));

//        sourceCSV.printSchema();
//        Dataset<Row> summary = sourceCSV.summary();
//        summary.show();

        Dataset<Person> beansDS = sourceCSV.as(personEncoder);
//        beansDS.printSchema();
        Dataset<Tuple2<Person, Company>> tuple2Dataset
                = beansDS.joinWith(companies, beansDS.col("companyId").equalTo(companies.col("id")));

        Dataset<Person> personsWithCompanies = tuple2Dataset.map(new CombinePersonAndCompanyFunction(), personEncoder);

//        System.out.println("Count:" + personsWithCompanies.count());
//        personsWithCompanies.show();
        KeyValueGroupedDataset<String, Person> grouped = personsWithCompanies
                .groupByKey(new PersonCompanyExtractor()
                        , Encoders.STRING());
//        grouped.count().show();
        Dataset<CompanyPerson> companyPersonDataset = grouped.flatMapGroups(stringPersonCompanyPersonFlatMapGroupsFunction
                , Encoders.bean(CompanyPerson.class));
//        companyPersonDataset.show();
        
        Dataset<CompanyWithLeader> withLeaderDataset
                = companyPersonDataset.map(new GetLeaderFunction(), Encoders.bean(CompanyWithLeader.class));
        withLeaderDataset.show();
    }
    private static final FlatMapGroupsFunction<String, Person, CompanyPerson> stringPersonCompanyPersonFlatMapGroupsFunction = (s, iterator) -> {
        CompanyPerson companyPerson = new CompanyPerson();
        companyPerson.setCompany(s);
        List<Person> people = new ArrayList<>();
        iterator.forEachRemaining(people::add);
        companyPerson.setPersonList(people);
        ArrayList<CompanyPerson> companyPeople = new ArrayList<>();
        companyPeople.add(companyPerson);
        return companyPeople.iterator();
    };

}

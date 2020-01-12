package ru.open.test.spark.demo;

import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;

public class CombinePersonAndCompanyFunction implements Function1<Tuple2<Person, Company>, Person>, Serializable {
    @Override
    public Person apply(Tuple2<Person, Company> v1) {
        Person person = v1._1;
        person.setCompany(v1._2.getName());
        return person;
    }
}

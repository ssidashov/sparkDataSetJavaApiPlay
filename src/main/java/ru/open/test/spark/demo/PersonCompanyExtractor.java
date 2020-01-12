package ru.open.test.spark.demo;

import scala.Function1;

import java.io.Serializable;

public class PersonCompanyExtractor implements Function1<Person, String>, Serializable {
    @Override
    public String apply(Person person) {
        return person.getCompany();
    }
}

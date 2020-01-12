package ru.open.test.spark.demo;

import java.io.Serializable;
import java.util.List;

public class CompanyPerson implements Serializable {
    private String company;
    private List<Person> personList;

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public List<Person> getPersonList() {
        return personList;
    }

    public void setPersonList(List<Person> personList) {
        this.personList = personList;
    }
}

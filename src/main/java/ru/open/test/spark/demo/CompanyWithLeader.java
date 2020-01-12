package ru.open.test.spark.demo;

import java.io.Serializable;

public class CompanyWithLeader implements Serializable {
    private String company;
    private Person leader;

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public Person getLeader() {
        return leader;
    }

    public void setLeader(Person leader) {
        this.leader = leader;
    }
}

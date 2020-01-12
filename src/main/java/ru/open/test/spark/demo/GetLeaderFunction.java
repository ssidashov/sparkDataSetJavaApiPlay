package ru.open.test.spark.demo;

import scala.Function1;

import java.io.Serializable;

public class GetLeaderFunction implements Function1<CompanyPerson, CompanyWithLeader>, Serializable {
    @Override
    public CompanyWithLeader apply(CompanyPerson v1) {
        CompanyWithLeader companyWithLeader = new CompanyWithLeader();
        companyWithLeader.setCompany(v1.getCompany());
        companyWithLeader.setLeader(v1.getPersonList().get(0));
        return companyWithLeader;
    }
}

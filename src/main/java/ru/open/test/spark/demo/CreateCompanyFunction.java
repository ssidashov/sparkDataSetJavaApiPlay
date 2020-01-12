package ru.open.test.spark.demo;

import org.apache.spark.sql.Row;
import scala.Function1;

import java.io.Serializable;

public class CreateCompanyFunction implements Function1<Row, Company>, Serializable {

    @Override
    public Company apply(Row v1) {
        Company company = new Company();
        company.setId(v1.getInt(0));
        company.setName(v1.getString(1));
        return company;
    }
}

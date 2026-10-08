package com.smartship.shore.persistence;

import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/** Real MyBatis mapper on each fixture DataSource; JDBC is only for test setup/assertions. */
public final class MyBatisTestSupport {
    private MyBatisTestSupport() {}

    public static TelemetryHistoryRepository repository(JdbcTemplate jdbc) {
        try {
            var factoryBean = new SqlSessionFactoryBean();
            factoryBean.setDataSource(jdbc.getDataSource());
            var configuration = new org.apache.ibatis.session.Configuration();
            configuration.setCallSettersOnNulls(true);
            configuration.addMapper(TelemetryHistoryMapper.class);
            factoryBean.setConfiguration(configuration);
            var session = new SqlSessionTemplate(factoryBean.getObject());
            return new TelemetryHistoryRepository(session.getMapper(TelemetryHistoryMapper.class),
                    new DataSourceTransactionManager(jdbc.getDataSource()));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create MyBatis fixture", e);
        }
    }

    public static TelemetryHistoryRepository repository(TelemetryHistoryRepository repository) { return repository; }
}

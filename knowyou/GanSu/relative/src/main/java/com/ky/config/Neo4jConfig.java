package com.ky.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author xwj
 * @className Neo4jConfig
 * @description TODO
 * @date 2020/9/1 9:50
 */
@Configuration
@EnableNeo4jRepositories(basePackages = "com.ky.repository")
@EnableTransactionManagement // 激活SDN隐式事务
public class Neo4jConfig {

}

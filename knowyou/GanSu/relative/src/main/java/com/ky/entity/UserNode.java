package com.ky.entity;


import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

/**
 * @author xwj
 * @className UserNode
 * @description TODO
 * @date 2020/8/31 20:00
 */
@NodeEntity(label = "User")
public class UserNode {
    @Id
    @GeneratedValue
    private Long nodeId;

    @Property(name = "userId")
    private String userId;

    @Property(name = "name")
    private String name;

    @Property(name = "age")
    private int age;


    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

}
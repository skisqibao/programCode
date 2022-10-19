package com.ky.entity;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

@NodeEntity(
   label = "User"
)
public class UserNode {
   @Id
   @GeneratedValue
   private Long nodeId;
   @Property(
      name = "userId"
   )
   private String userId;
   @Property(
      name = "name"
   )
   private String name;
   @Property(
      name = "age"
   )
   private int age;

   public Long getNodeId() {
      return this.nodeId;
   }

   public void setNodeId(Long nodeId) {
      this.nodeId = nodeId;
   }

   public String getUserId() {
      return this.userId;
   }

   public void setUserId(String userId) {
      this.userId = userId;
   }

   public String getName() {
      return this.name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public int getAge() {
      return this.age;
   }

   public void setAge(int age) {
      this.age = age;
   }
}

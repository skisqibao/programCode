package com.ky.entity;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

@NodeEntity(
   label = "Media"
)
public class MediaNode {
   @Id
   @GeneratedValue
   private Long id;
   @Property(
      name = "videoId"
   )
   private String videoId;
   @Property(
      name = "videoName"
   )
   private String videoName;
   @Property(
      name = "videoType"
   )
   private String videoType;
   @Property(
      name = "directName"
   )
   private String director;
   @Property(
      name = "videoPlot"
   )
   private String plot;
   @Property(
      name = "videoRegion"
   )
   private String region;
   @Property(
      name = "actorName"
   )
   private String actor;
   @Property(
      name = "videoScore"
   )
   private String videoScore;
   @Property(
      name = "score"
   )
   private Long score;
   @Property(
      name = "cpType"
   )
   private String cpType;

   public String getVideoId() {
      return this.videoId;
   }

   public void setVideoId(String videoId) {
      this.videoId = videoId;
   }

   public String getVideoName() {
      return this.videoName;
   }

   public void setVideoName(String videoName) {
      this.videoName = videoName;
   }

   public String getVideoType() {
      return this.videoType;
   }

   public void setVideoType(String videoType) {
      this.videoType = videoType;
   }

   public String getDirector() {
      return this.director;
   }

   public void setDirector(String director) {
      this.director = director;
   }

   public String getPlot() {
      return this.plot;
   }

   public void setPlot(String plot) {
      this.plot = plot;
   }

   public String getRegion() {
      return this.region;
   }

   public void setRegion(String region) {
      this.region = region;
   }

   public String getActor() {
      return this.actor;
   }

   public void setActor(String actor) {
      this.actor = actor;
   }

   public String getVideoScore() {
      return this.videoScore;
   }

   public void setVideoScore(String videoScore) {
      this.videoScore = videoScore;
   }

   public Long getId() {
      return this.id;
   }

   public void setId(Long id) {
      this.id = id;
   }

   public Long getScore() {
      return this.score;
   }

   public void setScore(Long score) {
      this.score = score;
   }

   public String getCpType() {
      return this.cpType;
   }

   public void setCpType(String cpType) {
      this.cpType = cpType;
   }

   public String toString() {
      return "MediaNode{id=" + this.id + ", videoId='" + this.videoId + '\'' + ", videoName='" + this.videoName + '\'' + ", videoType='" + this.videoType + '\'' + ", director='" + this.director + '\'' + ", plot='" + this.plot + '\'' + ", region='" + this.region + '\'' + ", actor='" + this.actor + '\'' + ", videoScore='" + this.videoScore + '\'' + ", score=" + this.score + ", cpType='" + this.cpType + '\'' + '}';
   }
}

package com.ky.entity.vo;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class MediaInfo implements Serializable {
   private String videoId;
   private String videoName;
   private String videoType;
   private List<String> driectorList;
   private List<String> plotList;
   private Set<String> regions;
   private List<String> actorList;
   private String videoScore = "0";
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

   public List<String> getDriectorList() {
      return this.driectorList;
   }

   public void setDriectorList(List<String> driectorList) {
      this.driectorList = driectorList;
   }

   public List<String> getPlotList() {
      return this.plotList;
   }

   public void setPlotList(List<String> plotList) {
      this.plotList = plotList;
   }

   public Set<String> getRegions() {
      return this.regions;
   }

   public void setRegions(Set<String> regions) {
      this.regions = regions;
   }

   public List<String> getActorList() {
      return this.actorList;
   }

   public void setActorList(List<String> actorList) {
      this.actorList = actorList;
   }

   public String getVideoScore() {
      return this.videoScore;
   }

   public void setVideoScore(String videoScore) {
      this.videoScore = videoScore;
   }

   public String getCpType() {
      return this.cpType;
   }

   public void setCpType(String cpType) {
      this.cpType = cpType;
   }

   public String toString() {
      return "MediaInfo{videoId='" + this.videoId + '\'' + ", videoName='" + this.videoName + '\'' + ", videoType='" + this.videoType + '\'' + ", driectorList=" + this.driectorList + ", plotList=" + this.plotList + ", regions=" + this.regions + ", actorList=" + this.actorList + ", videoScore='" + this.videoScore + '\'' + '}';
   }
}

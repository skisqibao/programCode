package com.ky.entity;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

/**
 * @author xwj
 * @className MediaNode
 * @description TODO
 * @date 2020/9/1 13:38
 */
@NodeEntity(label = "Media")
public class MediaNode {
    @Id
    @GeneratedValue
    private Long id;

    @Property(name = "videoId")
    private String videoId;

    @Property(name = "videoName")
    private String videoName;

    @Property(name = "videoType")
    private String videoType;

    @Property(name = "directName")
    private String director;

    @Property(name = "videoPlot")
    private String plot;

    @Property(name = "videoRegion")
    private String region;

    @Property(name = "actorName")
    private String actor;

    @Property(name = "videoScore")
    private String videoScore;

    @Property(name = "score")
    private Long score;

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getVideoName() {
        return videoName;
    }

    public void setVideoName(String videoName) {
        this.videoName = videoName;
    }

    public String getVideoType() {
        return videoType;
    }

    public void setVideoType(String videoType) {
        this.videoType = videoType;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getPlot() {
        return plot;
    }

    public void setPlot(String plot) {
        this.plot = plot;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public String getVideoScore() {
        return videoScore;
    }

    public void setVideoScore(String videoScore) {
        this.videoScore = videoScore;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getScore() {
        return score;
    }

    public void setScore(Long score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "MediaNode{" +
                "videoId='" + videoId + '\'' +
                ", videoName='" + videoName + '\'' +
                ", videoType='" + videoType + '\'' +
                ", director='" + director + '\'' +
                ", plot='" + plot + '\'' +
                ", region='" + region + '\'' +
                ", actor='" + actor + '\'' +
                ", videoScore='" + videoScore + '\'' +
                '}';
    }
}

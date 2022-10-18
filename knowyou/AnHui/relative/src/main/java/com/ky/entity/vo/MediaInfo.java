package com.ky.entity.vo;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * @author xwj
 * @className MediaInfo
 * @description TODO
 * @date 2020/9/1 11:20
 */
public class MediaInfo implements Serializable {
    private String videoId;
    private String videoName;
    private String videoType;
    private List<String> driectorList;
    private List<String> plotList;
    private Set<String> regions;
    private List<String> actorList;
    private String videoScore;

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

    public List<String> getDriectorList() {
        return driectorList;
    }

    public void setDriectorList(List<String> driectorList) {
        this.driectorList = driectorList;
    }

    public List<String> getPlotList() {
        return plotList;
    }

    public void setPlotList(List<String> plotList) {
        this.plotList = plotList;
    }

    public Set<String> getRegions() {
        return regions;
    }

    public void setRegions(Set<String> regions) {
        this.regions = regions;
    }

    public List<String> getActorList() {
        return actorList;
    }

    public void setActorList(List<String> actorList) {
        this.actorList = actorList;
    }

    public String getVideoScore() {
        return videoScore;
    }

    public void setVideoScore(String videoScore) {
        this.videoScore = videoScore;
    }

    @Override
    public String toString() {
        return "MediaInfo{" +
                "videoId='" + videoId + '\'' +
                ", videoName='" + videoName + '\'' +
                ", videoType='" + videoType + '\'' +
                ", driectorList=" + driectorList +
                ", plotList=" + plotList +
                ", regions=" + regions +
                ", actorList=" + actorList +
                ", videoScore='" + videoScore + '\'' +
                '}';
    }
}

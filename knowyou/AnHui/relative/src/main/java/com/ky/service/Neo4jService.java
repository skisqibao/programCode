package com.ky.service;

import com.ky.entity.MediaNode;
import com.ky.entity.UserNode;
import com.ky.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author xwj
 * @className Neo4jService
 * @description TODO
 * @date 2020/9/1 9:51
 */
@Service
public class Neo4jService {

    @Autowired
    private UserRepository userRepository;

    public void addUser(UserNode userNode) {
        userRepository.addUserNodeList(userNode.getName(), userNode.getAge());
    }

    public List<UserNode> getUserNodeList() {
        return userRepository.getUserNodeList();
    }

    public void addMedia(MediaNode mediaNode) {
        userRepository.addMediaNodeList(mediaNode.getVideoId(), mediaNode.getVideoName(), mediaNode.getVideoScore(), mediaNode.getRegion(), mediaNode.getVideoType(), mediaNode.getDirector(), mediaNode.getActor(), mediaNode.getPlot());
    }

    public List<MediaNode> getRelativeVideos(String videoId) {
        return userRepository.getRelativeVideos(videoId);
    }
}
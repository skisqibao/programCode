package com.ky.service;

import com.ky.entity.MediaNode;
import com.ky.entity.UserNode;
import com.ky.repository.UserRepository;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Neo4jService {
   @Autowired
   private UserRepository userRepository;

   public void addUser(UserNode userNode) {
      this.userRepository.addUserNodeList(userNode.getName(), userNode.getAge());
   }

   public List<UserNode> getUserNodeList() {
      return this.userRepository.getUserNodeList();
   }

   public void addMedia(MediaNode mediaNode) {
      this.userRepository.addMediaNodeList(mediaNode.getVideoId(), mediaNode.getVideoName(), mediaNode.getVideoScore(), mediaNode.getRegion(), mediaNode.getVideoType(), mediaNode.getDirector(), mediaNode.getActor(), mediaNode.getPlot(), mediaNode.getCpType());
   }

   public List<MediaNode> getRelativeVideos(String videoId) {
      return this.userRepository.getRelativeVideos(videoId);
   }
}

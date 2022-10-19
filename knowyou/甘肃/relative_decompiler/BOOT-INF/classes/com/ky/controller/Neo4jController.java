package com.ky.controller;

import com.ky.common.CommonUtil;
import com.ky.common.ResponseResult;
import com.ky.common.ResponseResultUtil;
import com.ky.entity.MediaNode;
import com.ky.entity.UserNode;
import com.ky.entity.vo.MediaInfo;
import com.ky.hbase.HBase;
import com.ky.hbase.HBaseUtil;
import com.ky.service.Neo4jService;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Neo4jController {
   private static final Logger LOG = LoggerFactory.getLogger(Neo4jController.class);
   @Autowired
   private Neo4jService neo4jService;
   @Value("${com.ky.hbase.mediaTable}")
   private String mediaTable;
   @Value("${com.ky.hbase.relativeTable}")
   private String relativeTable;

   @RequestMapping(
      path = {"/addUserNode"},
      method = {RequestMethod.GET}
   )
   public ResponseResult addUserNode() {
      int i = 0;

      do {
         UserNode user = new UserNode();
         user.setAge(RandomUtils.nextInt(15, 40));
         user.setName("Fredia" + RandomUtils.nextInt(1, 1000));
         user.setUserId(UUID.randomUUID().toString());
         user.setNodeId(RandomUtils.nextLong(1L, 999L));
         this.neo4jService.addUser(user);
         ++i;
      } while(i < 400);

      return ResponseResultUtil.getSuccessRequest("sucess add user");
   }

   @RequestMapping(
      path = {"/getUserNodeList"},
      method = {RequestMethod.GET}
   )
   public List<UserNode> getUserNodeList() {
      return this.neo4jService.getUserNodeList();
   }

   @RequestMapping(
      path = {"/addMedia"},
      method = {RequestMethod.GET}
   )
   @Scheduled(
      cron = "0 0 12 * * ?"
   )
   public void addMedia() {
      List<MediaInfo> recommendList = HBaseUtil.getRecommendList(this.mediaTable);
      List<MediaNode> mediaInfos = CommonUtil.getMediaInfos(recommendList);
      if (mediaInfos.size() > 0) {
         mediaInfos.forEach((mediaNode) -> {
            this.neo4jService.addMedia(mediaNode);
         });
      }

   }

   @RequestMapping(
      path = {"/addRecommend"},
      method = {RequestMethod.GET}
   )
   @Scheduled(
      cron = "0 0 19 * * ?"
   )
   public void addRecommend() {
      try {
         HBaseUtil.createTable(this.relativeTable, new String[]{"info"}, false);
      } catch (Exception var2) {
         var2.printStackTrace();
      }

      HBaseUtil.getRecommendList(this.mediaTable).forEach((mediaInfo) -> {
         if (StringUtils.isNotEmpty(mediaInfo.getCpType()) && StringUtils.isNotEmpty(mediaInfo.getVideoId())) {
            List<MediaNode> relativeVideos = this.neo4jService.getRelativeVideos(mediaInfo.getVideoId());
            if (relativeVideos.size() > 0) {
               String recommendList = (String)relativeVideos.stream().filter((s) -> {
                  return s.getCpType().equalsIgnoreCase(mediaInfo.getCpType());
               }).map(MediaNode::getVideoId).limit(20L).collect(Collectors.joining(","));
               LOG.info("current recommend video {}", recommendList);
               if (StringUtils.isNotEmpty(recommendList)) {
                  HBase.put(String.join("_", mediaInfo.getCpType(), mediaInfo.getVideoId()), this.relativeTable, "info", new String[]{"recommend"}, new String[]{recommendList}, false);
               }
            }
         }

      });
   }
}

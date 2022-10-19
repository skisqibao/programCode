package com.ky.controller;

import com.ky.common.ResponseResult;
import com.ky.common.ResponseResultUtil;
import com.ky.entity.MediaNode;
import com.ky.entity.UserNode;
import com.ky.entity.vo.MediaInfo;
import com.ky.hbase.HBase;
import com.ky.hbase.HBaseUtil;
import com.ky.service.Neo4jService;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ky.common.CommonUtil.getMediaInfos;
import static com.ky.hbase.HBaseUtil.getRecommendList;

/**
 * @author xwj
 * @className Neo4jController
 * @description TODO
 * @date 2020/9/1 10:01
 */
@RestController
public class Neo4jController {

    @Autowired
    private Neo4jService neo4jService;

    @Value("${com.ky.hbase.mediaTable}")
    private String mediaTable;

    @Value("${com.ky.hbase.relativeTable}")
    private String relativeTable;

    //创建400个node
    @RequestMapping(path = "/addUserNode", method = RequestMethod.GET)
    public ResponseResult addUserNode() {
        int i = 0;
        do {
            UserNode user = new UserNode();
            user.setAge(RandomUtils.nextInt(15, 40));
            user.setName("Fredia" + RandomUtils.nextInt(1, 1000));
            user.setUserId(UUID.randomUUID().toString());
            user.setNodeId(RandomUtils.nextLong(1L, 999L));
            neo4jService.addUser(user);
            i += 1;
        } while (i < 400);
        return ResponseResultUtil.getSuccessRequest("sucess add user");
    }

    @RequestMapping(path = "/getUserNodeList", method = RequestMethod.GET)
    public List<UserNode> getUserNodeList() {
        return neo4jService.getUserNodeList();
    }

    @RequestMapping(path = "/addMedia", method = RequestMethod.GET)
    @Scheduled(cron = "0 0 12 * * ?")
    public void addMedia() {
        List<MediaInfo> recommendList = getRecommendList(mediaTable);
        List<MediaNode> mediaInfos = getMediaInfos(recommendList);
        if (mediaInfos.size() > 0) {
            mediaInfos.forEach(mediaNode -> {
                neo4jService.addMedia(mediaNode);
            });
        }
    }


    @RequestMapping(path = "/addRecommend", method = RequestMethod.GET)
    @Scheduled(cron = "0 0 19 * * ?")
    public void addRecommend() {
        try {
            HBaseUtil.createTable(relativeTable, new String[]{"info"}, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<String> videoList = getRecommendList(mediaTable).stream().map(MediaInfo::getVideoId).collect(Collectors.toList());
        videoList.forEach(videoId -> {
            List<MediaNode> relativeVideos = neo4jService.getRelativeVideos(videoId);
            String recommendList = relativeVideos.stream().map(MediaNode::getVideoId).collect(Collectors.joining(","));
            HBase.put(videoId, relativeTable, "info", new String[]{"recommend"}, new String[]{recommendList}, false);
        });
    }

}

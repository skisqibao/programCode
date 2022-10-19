package com.ky.common;

import com.ky.entity.MediaNode;
import com.ky.entity.vo.MediaInfo;
import com.ky.hbase.HBase;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CommonUtil {
    private final static Logger LOG = LoggerFactory.getLogger(CommonUtil.class);

    //获取导演和演员的list
    public static List<String> getFilmerList(String str) {
        str = getFormateLine(str);
        final String[] temp = str.trim().split("[丨|/ ]");
        return Arrays.asList(temp);
    }

    //对逗号，分号，冒号，空格进行处理
    private static String getFormateLine(String value) {
        if (value.contains(CodeEnmu.COMMA.getDesc())) {
            value = value.replace(CodeEnmu.COMMA.getDesc(), CodeEnmu.AXIS.getDesc());
        } else if (value.contains(CodeEnmu.SEMICOLON.getDesc())) {
            value = value.replace(CodeEnmu.SEMICOLON.getDesc(), CodeEnmu.AXIS.getDesc());
        } else if (value.contains(CodeEnmu.COLON.getDesc())) {
            value = value.replace(CodeEnmu.COLON.getDesc(), CodeEnmu.AXIS.getDesc());
        } else if (value.contains(CodeEnmu.DOLLAR.getDesc())) {
            value = value.replace(CodeEnmu.DOLLAR.getDesc(), CodeEnmu.AXIS.getDesc());
        } else if (value.contains(CodeEnmu.BLANK.getDesc())) {
            value = value.replace(CodeEnmu.BLANK.getDesc(), CodeEnmu.AXIS.getDesc());
        }
        return value;
    }


    //评分转换
    public static String getCleanScore(String score) {
        String result = "";
        try {
            if (StringUtils.isNotEmpty(score)) {
                result = String.format("%.1f", Double.valueOf(score));
            }
        } catch (NumberFormatException e) {
            LOG.error("the score is not number,please check the raw data {},ex {}", score, e.getMessage());
        }
        return result;
    }

    //地域转换
    public static Set<String> getCleanRegion(String region) {
        final String[] temp = region.trim().split("[丨|/ ]");
        final List<String> chinaList = Arrays.asList("中国大陆", "内地", "中国内地", "中国", "大陆", "国语大陆", "china", "chinese mainland", "mainland");
        final List<String> hongKongList = Arrays.asList("香港", "中国香港");
        final List<String> taiwaiList = Arrays.asList("台湾", "中国台湾");
        final List<String> japanList = Collections.singletonList("日语");
        final List<String> usList = Arrays.asList("欧美", "USA", "usa");
        final List<String> japanAndKorean = Collections.singletonList("日韩");
        final List<String> notUseList = Arrays.asList("Aruba", "meiguo", "无", "");
        final HashSet<String> regionList = new HashSet<>();
        for (String place : temp) {
            if (chinaList.contains(place)) {
                regionList.add("中国大陆");
            } else if (hongKongList.contains(place)) {
                regionList.add("香港");
            } else if (taiwaiList.contains(place)) {
                regionList.add("台湾");
            } else if (japanList.contains(place)) {
                regionList.add("日本");
            } else if (usList.contains(place)) {
                regionList.add("美国");
            } else if (japanAndKorean.contains(place)) {
                regionList.add("日本");
                regionList.add("韩国");
            } else if (notUseList.contains(place)) {
                System.out.println("the place is not exist");
            } else {
                regionList.add(place);
            }
        }
        return regionList;
    }

    //获取mediaInfo的集合
    public static List<MediaNode> getMediaInfos(List<MediaInfo> recommendList) {
        List<MediaNode> mediaNodes = new ArrayList<>();
        recommendList.forEach(mediaInfo -> {
            LOG.info(mediaInfo.toString());
            try {
                List<String> actorList = mediaInfo.getActorList();
                Set<String> regions = mediaInfo.getRegions();
                List<String> plotList = mediaInfo.getPlotList();
                List<String> driectorList = mediaInfo.getDriectorList();
                for (String actor : actorList) {
                    for (String region : regions) {
                        for (String plot : plotList) {
                            for (String director : driectorList) {
                                MediaNode mediaNode = new MediaNode();
                                mediaNode.setVideoId(mediaInfo.getVideoId());
                                mediaNode.setActor(getFilterValue(actor));
                                mediaNode.setRegion(getFilterValue(region));
                                mediaNode.setPlot(getFilterValue(plot));
                                mediaNode.setDirector(getFilterValue(director));
                                mediaNode.setVideoScore(mediaInfo.getVideoScore());
                                mediaNode.setVideoType(getFilterValue(mediaInfo.getVideoType()));
                                mediaNode.setVideoName(getFilterValue(mediaInfo.getVideoName()));
                                System.out.println(mediaNode);
                                mediaNodes.add(mediaNode);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("getMediaInfos error {}", e.getMessage());
            }
        });
        return mediaNodes;
    }

    private static String getFilterValue(String value) {
        if (StringUtils.isEmpty(value) || "无".equalsIgnoreCase(value) || "$".equalsIgnoreCase(value)) {
            return "其他";
        }
        return value;
    }
}

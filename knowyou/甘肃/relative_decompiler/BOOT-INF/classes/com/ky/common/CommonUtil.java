package com.ky.common;

import com.ky.entity.MediaNode;
import com.ky.entity.vo.MediaInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtil {
   private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);

   public static List<String> getFilmerList(String str) {
      str = getFormateLine(str);
      String[] temp = str.trim().split("[丨|/ ]");
      return Arrays.asList(temp);
   }

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

   public static String getCleanScore(String score) {
      String result = "";

      try {
         if (StringUtils.isNotEmpty(score)) {
            result = String.format("%.1f", Double.valueOf(score));
         }
      } catch (NumberFormatException var3) {
         LOG.error("the score is not number,please check the raw data {},ex {}", score, var3.getMessage());
      }

      return result;
   }

   public static Set<String> getCleanRegion(String region) {
      String[] temp = region.trim().split("[丨|/ ]");
      List<String> chinaList = Arrays.asList("中国大陆", "内地", "中国内地", "中国", "大陆", "国语大陆", "china", "chinese mainland", "mainland");
      List<String> hongKongList = Arrays.asList("香港", "中国香港");
      List<String> taiwaiList = Arrays.asList("台湾", "中国台湾");
      List<String> japanList = Collections.singletonList("日语");
      List<String> usList = Arrays.asList("欧美", "USA", "usa");
      List<String> japanAndKorean = Collections.singletonList("日韩");
      List<String> notUseList = Arrays.asList("Aruba", "meiguo", "无", "");
      HashSet<String> regionList = new HashSet();
      String[] var10 = temp;
      int var11 = temp.length;

      for(int var12 = 0; var12 < var11; ++var12) {
         String place = var10[var12];
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

   public static List<MediaNode> getMediaInfos(List<MediaInfo> recommendList) {
      List<MediaNode> mediaNodes = new ArrayList();
      if (recommendList.size() > 0) {
         recommendList.forEach((mediaInfo) -> {
            LOG.info(mediaInfo.toString());

            try {
               List<String> actorList = mediaInfo.getActorList();
               Set<String> regions = mediaInfo.getRegions();
               List<String> plotList = mediaInfo.getPlotList();
               List<String> driectorList = mediaInfo.getDriectorList();
               Iterator var6 = actorList.iterator();

               while(var6.hasNext()) {
                  String actor = (String)var6.next();
                  Iterator var8 = regions.iterator();

                  while(var8.hasNext()) {
                     String region = (String)var8.next();
                     Iterator var10 = plotList.iterator();

                     while(var10.hasNext()) {
                        String plot = (String)var10.next();
                        Iterator var12 = driectorList.iterator();

                        while(var12.hasNext()) {
                           String director = (String)var12.next();
                           MediaNode mediaNode = new MediaNode();
                           mediaNode.setVideoId(mediaInfo.getVideoId());
                           mediaNode.setActor(getFilterValue(actor));
                           mediaNode.setRegion(getFilterValue(region));
                           mediaNode.setPlot(getFilterValue(plot));
                           mediaNode.setDirector(getFilterValue(director));
                           mediaNode.setVideoScore(mediaInfo.getVideoScore());
                           mediaNode.setVideoType(getFilterValue(mediaInfo.getVideoType()));
                           mediaNode.setVideoName(getFilterValue(mediaInfo.getVideoName()));
                           mediaNode.setCpType(getFilterValue(mediaInfo.getCpType()));
                           mediaNodes.add(mediaNode);
                        }
                     }
                  }
               }
            } catch (Exception var15) {
               LOG.error("getMediaInfos error {}", var15.getMessage());
            }

         });
      }

      return mediaNodes;
   }

   private static String getFilterValue(String value) {
      return !StringUtils.isEmpty(value) && !"无".equalsIgnoreCase(value) && !"$".equalsIgnoreCase(value) && !"null".equalsIgnoreCase(value) ? value : "other";
   }
}

package com.knowyou.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

/**
 * @author Knowyou
 */
public class PosterSizeTask {
    public static void main(String[] args) throws IOException {
        String filePath = "D:\\temp1.txt";
        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        FileOutputStream fileOutputStream = new FileOutputStream("D:\\result2.txt", true);
        String line;
        int i = 1;
        while ((line = br.readLine()) != null) {
            if (i <= 107877) {
                i = i + 1;
                continue;
            }
            String[] split = line.split("\t");
            String seriescode = split[0];
            String seriesAction = split[1];
            String seriesName = split[2];
            String picurl = null;
            String img="";
            try {
                if (split.length >= 4) {
                    picurl = split[3];
                }
                if (picurl != null || !picurl.equalsIgnoreCase("NULL") ) {
                    img = getImg(picurl);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            String res = String.join(",", seriescode, seriesAction, seriesName, picurl, img, "\r\n");
            fileOutputStream.write(res.getBytes(StandardCharsets.UTF_8));
            i = i + 1;
            System.out.println("已处理文件数：" + i);
        }
        System.out.println("处理结束，总处理数据量：" + i);
    }

    private static String getImg(String imageUrl) {
        try {
            URL url = new URL(imageUrl);
            URLConnection connection = url.openConnection();
            connection.setConnectTimeout(100);
            connection.setDoOutput(true);
            BufferedImage image = ImageIO.read(connection.getInputStream());
            int srcWidth = image.getWidth();
            int srcHeight = image.getHeight();
            System.out.println("width:" + srcWidth);
            System.out.println("height:" + srcHeight);
            return String.join(",", String.valueOf(srcWidth), String.valueOf(srcHeight));
        } catch (Exception e) {
            System.out.println("pic error:" + imageUrl);
        }
        return null;
    }

}

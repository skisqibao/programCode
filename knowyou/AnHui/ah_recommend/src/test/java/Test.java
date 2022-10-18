import com.google.common.collect.Lists;
import com.knowyou.util.CateName;
import com.knowyou.util.FormatTime;
import com.knowyou.util.FreePayType;
import org.apache.spark.broadcast.Broadcast;
import scala.reflect.ClassManifestFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Test {
    public static void main(String[] args) {
        String currentDay = FormatTime.getCurrentDay();
        String sevenDayBefore = FormatTime.fTime(currentDay, 7);
        String sqlBase = String.format("select  '' as deviceid,b.seriesheadcode as videoid,'' as rating,b.rank " +
                "from( select a.seriesheadcode,a.series_type, " +
                "row_number () over( partition by a.series_type ORDER BY a.playnum DESC) AS rank " +
                "from ( select seriesheadcode, series_type ,count(*) as playnum " +
                "from knowyou_ott_ods.dws_rec_video_playinfo_di where dt>='%s' and dt <='%s' ", sevenDayBefore, currentDay);
        String sqlEnd = "group by seriesheadcode,series_type )a )b where b.rank<=10";
        List<HashMap<String, String>> cateNameList = CateName.toList();
        List<HashMap<String, String>> freePayTypeList = FreePayType.toList();
        for (HashMap<String, String> cateNameMap : cateNameList) {
            for (HashMap<String, String> freePayTypeMap : freePayTypeList) {
                String cateName = cateNameMap.get("cateName");
                String freePaySql = freePayTypeMap.get("sql");
                //TODO 新增freePayType广播变量
                String dataSource = "";
                if ("all".equalsIgnoreCase(cateName)) {
                    dataSource = String.format(sqlBase + " %s " + sqlEnd, freePaySql);
                } else {
                    dataSource = String.format(sqlBase + " and series_type='%s' %s " + sqlEnd, cateName, freePaySql);
                }
                System.out.println(dataSource);


            }
        }

    }
}

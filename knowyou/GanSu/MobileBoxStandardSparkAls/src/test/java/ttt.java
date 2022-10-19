import org.apache.spark.mllib.recommendation.Rating;

import java.util.ArrayList;

public class ttt {
    public static void main(String[] args) {
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        ArrayList<Object> aa = new ArrayList<>();
        aa.add(o1);
        aa.add(o2);
        aa.add(o3);
        Object[] aaa = new Object[aa.size()];
        Object[] strings = aa.toArray(aaa);

        System.out.println(aaa.length + aaa.toString());

    }
}

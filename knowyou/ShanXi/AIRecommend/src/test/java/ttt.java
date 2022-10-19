import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.LinkedList;

public class ttt {

    public static void main(String[] args) {



        Groups group = new Groups();
        group.setName("BeJson");
        group.setUrl("http://www.bejson.com");
        group.setPage(88);
        group.setNonProfit(true);
        HashMap<String, String> mapDemo = new HashMap<>();
        mapDemo.put("street","科技园路");
        mapDemo.put("city","江苏苏州");
        mapDemo.put("country","中国");
        group.setAddress(mapDemo);
        LinkBean linkBean1 = new LinkBean();
        linkBean1.setName("google");
        linkBean1.setUrl("http://www.google.com");
        LinkBean linkBean2 = new LinkBean();
        linkBean2.setName("baidu");
        linkBean2.setUrl("http://www.baidu.com");
        LinkBean linkBean3 = new LinkBean();
        linkBean3.setName("soso");
        linkBean3.setUrl("http://www.soso.com");

        group.getList().add(linkBean1);
        group.getList().add(linkBean2);
        group.getList().add(linkBean3);

        String s = JSON.toJSONString(group);
        System.out.println(s);
//        Object o1 = new Object();
//        Object o2 = new Object();
//        Object o3 = new Object();
//        ArrayList<Long> aa = new ArrayList<>();
//        ArrayList<Long> bb = new ArrayList<>();
//        aa.add(1L);
//        bb.add(1L);
//        bb.add(2L);
//
//        int hit = aa.stream().filter(bb::contains).collect(Collectors.toList()).size();
////        List<String> collect = aa.stream().distinct().collect(Collectors.toList());
////        System.out.println(aa.toString());
//        System.out.println(hit);
//
//        int a =3;
//        Object ccc = 20L;
//
//        Long b= 20L;
//        Long c= 2022L;
//        ArrayList<Long> objects = new ArrayList<>();
//        objects.add(20L);
//        objects.add(202L);
//        System.out.println("aaaaaaaa===" + (objects.contains(Long.valueOf(String.valueOf(ccc))) && objects.contains(Long.valueOf(String.valueOf(c)))));
//        System.out.println(b > a);
//        Boolean aaa = true;
//        System.out.println(!(aaa));
//        System.out.println(objects.toString());
//        String res = "";
//        for (Long x : objects) {
//            res = res + "," + String.valueOf(x);
//        }
//        String s1 = objects.toString()
//                .replaceAll("\\]", " ")
//                .replaceAll("\\[", " ").replaceAll(" ", "");
//        System.out.println(s1);
//        String z = "1,2";
//        String[] split = z.split(",");
//        String s = split[0];
//        String ss = split[1];
//        System.out.println(split[0]);
//        System.out.println(split[0]);
//        String ccca = "20201202";
//        String substring = ccca.substring(0, 6);
//        System.out.println(substring);
//        System.out.println(collect.toString());
//        aa.add(o3);
//        Object[] aaa = new Object[aa.size()];
//        Object[] strings = aa.toArray(aaa);

//        System.out.println(aaa.length + aaa.toString());
//        double meanF1score = 0;
//        meanF1score /= aa.size();
//        System.out.println("========="+meanF1score);

    }
}

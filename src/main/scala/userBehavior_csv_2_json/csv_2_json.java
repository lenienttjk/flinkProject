package userBehavior_csv_2_json;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @Auther: tjk
 * @Date: 2020-06-14 18:27
 * @Description:
 */

public class csv_2_json {


    public static void main(String[] args) {
// TODO Auto-generated method stub
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("E:\\Project\\flinkProject\\dataSource\\UserBehavior.csv"));
        } catch (FileNotFoundException e) {
// TODO Auto-generated catch block
            e.printStackTrace();
        }
        String str = "[";
        String temp="";
        try {
            while(null != (temp = br.readLine())){
                str +="{name:"+"\""+temp+"\""+", ticked: false},";
            }
        } catch (IOException e) {

            e.printStackTrace();
        }
        System.out.println(str+"]");
        try {
            br.close();
        } catch (IOException e) {

            e.printStackTrace();
        }
    }
}

// {"user_id": "543462", "item_id":"1715", "category_id": "1464116", "behavior": "pv", "ts": "2017-11-26T01:00:00Z"}
class  Bean{
    private String user_id;
    private String item_id;
    private String category_id;
    private String behavior;
    private String ts;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getItem_id() {
        return item_id;
    }

    public void setItem_id(String item_id) {
        this.item_id = item_id;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
}
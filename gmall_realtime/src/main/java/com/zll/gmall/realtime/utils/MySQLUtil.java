package com.zll.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import com.zll.gmall.realtime.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MySQLUtil
 * @Description TODO
 * @Author 17588
 * @Date 2021-06-01 10:00
 * @Version 1.0
 */
public class MySQLUtil {
    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean unserScoreToCamel){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
//            注册驱动
            Class.forName("com.mysql.jdbc.Driver");
//            建立连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "root");
//            创建数据库连接对象
            ps = conn.prepareStatement(sql);
//            执行SQL
            rs = ps.executeQuery();
//            处理结果集
            ResultSetMetaData md = rs.getMetaData();
//            声明集合对象，用于封装返回结果
            List<T> resultList = new ArrayList<>();
//            每循环一次，获取一条查询结果
            while (rs.next()) {
                //通过反射创建要将查询结果转换为目标类型的对象
                T obj = clazz.newInstance();

                //对查询出的列进行遍历，每遍历一次得到一个列名
                for (int i = 1; i < md.getColumnCount(); i++) {
                    String propertyName = md.getColumnName(i);
                    if (unserScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, propertyName);
                    }
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }
            return resultList;
    }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询MySQL失败");
        }finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
//测试程序
    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        for(TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}

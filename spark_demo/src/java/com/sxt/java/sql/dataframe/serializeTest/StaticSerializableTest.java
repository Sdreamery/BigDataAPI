package com.sxt.java.sql.dataframe.serializeTest;


import com.sxt.java.sql.dataframe.serializeTest.bean.Person;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 问题：静态变量能否被序列化？(不能被序列化)
 * <p>
 * 结论：  可以看到当改变静态变量的值时，序列化后的静态变量也是改变的，如果正常能被序列化的值，是在反序列化中得到的值是不会变的，可见，静态变量不能被序列化。
 * 那为什么反序列化可以读到值呢？
 * 一个静态变量不管是否被transient修饰，均不能被序列化，反序列化后类中static型变量isPerson的值为当前JVM中对应static变量的值，
 * 这个值是JVM中的,不是反序列化得出的
 *
 * @author root
 */
public class StaticSerializableTest {

    public static void main(String[] args) {
        Person person = new Person();
        Person.isPerson = "yes";

        person.setName("zhangsan");
        person.setAge("18");
        System.out.println("before serializable : ");
        System.out.println("isPerson : " + Person.isPerson);
        System.out.println("name : " + person.name);
        System.out.println("age : " + person.age);

        try {
//            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("data/personSerialize.txt"));
//            out.writeObject(person);
//            out.flush();
//            out.close();


            System.out.println("\n=================\n");

            ObjectInputStream in = new ObjectInputStream(new FileInputStream("data/personSerialize.txt"));

            person = (Person) in.readObject();


            in.close();

            System.out.println("after serializable : ");
            System.out.println("isPerson : " + person.isPerson);
            System.out.println("name : " + person.name);
            System.out.println("age : " + person.age);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}

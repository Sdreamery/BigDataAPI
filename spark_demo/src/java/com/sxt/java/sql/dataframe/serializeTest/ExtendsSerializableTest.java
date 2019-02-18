package com.sxt.java.sql.dataframe.serializeTest;

import com.sxt.java.sql.dataframe.serializeTest.bean.Apple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 测试：子类实现serializable 父类不实现serializable,当序列化子类时，反序列化得到父类中的某变量的数值时，该变量的数值与序列化时的数值不相同?（不相同）
 *
 * 步骤：  1.父类不实现serializable,子类实现serializable,父类中比子类中多一个common变量。
 * 		2.将子类序列化，然后反序列化，观察能否得到，设置父类中的变量common
 * 		3.将父类实现serializable,同样操作第二步骤，观察反序列化能否得到父类中的common值
 *
 * 结论：一个子类实现了 Serializable 接口，它的父类都没有实现 Serializable 接口，序列化该子类对象，然后反序列化后输出父类定义的某变量的数值，
 * 	该变量数值与序列化时的数值不同。（需要在父类中是实现默认的构造方法，否则会报异常:no validconstructor）
 * 	在父类没有实现 Serializable 接口时，虚拟机是不会序列化父对象的，而一个 Java 对象的构造必须先有父对象，才有子对象，反序列化也不例外。
 * 	所以反序列化时，为了构造父对象，只能调用父类的无参构造函数作为默认的父对象。因此当我们取父对象的变量值时，它的值是调用父类无参构造函数后的值。
 * 	如果你考虑到这种序列化的情况，在父类无参构造函数中对变量进行初始化，否则的话，父类变量值都是默认声明的值，如 int 型的默认是 0，string 型的默认是 null。
 *
 * 应用场景：
 * 	根据父类对象序列化的规则，我们可以将不需要被序列化的字段抽取出来放到父类中，子类实现 Serializable 接口，
 * 	父类不实现，根据父类序列化规则，父类的字段数据将不被序列化，
 *
 */

/**
 * 测试子类实现serializable接口，父类不实现serializable接口，父类中的变量不能 被序列化。
 *
 * 步骤：
 * 1.子类（Apple）实现serializable接口，父类（Fruit）不实现serializable接口。
 * 2.运行以下代码，观察结果。
 * @author root
 *
 */

public class ExtendsSerializableTest {
    public static void main(String[] args) {
        Apple apple = new Apple();
        apple.setCommon("in apple");
        apple.setColor("red");
        apple.setName("apple");
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("data/apple.txt"));
            out.writeObject(apple);
            out.flush();
            out.close();
            System.out.println("before serializable : ");
            System.out.println("apple common : " + apple.common);
            System.out.println("apple name  : " + apple.name);
            System.out.println("apple color : "+ apple.color);
            System.out.println("\n-----------------------\n");

            ObjectInputStream in = new ObjectInputStream(new FileInputStream("data/apple.txt"));

            apple = (Apple)in.readObject();
            System.out.println("after serializable :");
            System.out.println("apple common : "+ apple.common);
            System.out.println("apple name : "+ apple.name);
            System.out.println("apple color : "+ apple.color);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}

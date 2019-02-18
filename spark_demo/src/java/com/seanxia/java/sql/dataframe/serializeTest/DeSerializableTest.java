package com.seanxia.java.sql.dataframe.serializeTest;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


import com.seanxia.java.sql.dataframe.serializeTest.bean.User;

/**
 * 测试：反序列化时 实体类中的serialVersionUID  改变后能否反序列化？（不能）
 * <p>
 * 步骤：  1.将本地bean serialVersionUID 版本号改为1L，然后序列化
 * 2.将本地bean serialVersionUID 版本号改为2L，然后反序列化
 * 3.观察能否完成反序列化.
 * 结论：简单来说，Java的序列化机制是通过在运行时判断类的serialVersionUID来验证版本一致性的。在进行反序列化时，
 * JVM会把传来的字节流中的serialVersionUID与本地相应实体（类）的serialVersionUID进行比较，如果相同就认为是一致的，
 * 可以进行反序列化，否则就会出现序列化版本不一致的异常。
 * 当实现java.io.Serializable接口的实体（类）没有显式地定义一个名为serialVersionUID，类型为long的变量时，
 * Java序列化机制会根据编译的class自动生成一个serialVersionUID作序列化版本比较用，这种情况下，
 * 只有同一次编译生成的class才会生成相同的serialVersionUID 。如果我们不希望通过编译来强制划分软件版本，
 * 即实现序列化接口的实体能够兼容先前版本，未作更改的类，就需要显式地定义一个名为serialVersionUID，类型为long的变量，
 * 不修改这个变量值的序列化实体都可以相互进行串行化和反串行化。
 *
 * @author root
 */

public class DeSerializableTest {
    public static void main(String[] args) {
//        User user = new User();
//        user.setUsername("zhangsan");
//        user.setPasswd("1234");

        try {

            /**
             * 这里注意第一次运行的时候要把下面代码中的反序列部分注释，然后改正User对象中的serializableVersion的版本号，
             * 运行反序列化时，要把序列化代码注释，这样才能保证版本不一致。
             */

//			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("data/user.txt"));
//			out.writeObject(user);
//			out.flush();
//			out.close();
////
            ObjectInputStream in = new ObjectInputStream(new FileInputStream("data/user.txt"));
            User user = (User) in.readObject();

            System.out.println(user);
            System.out.println(user.getPasswd() + "   " + user.getUsername());
            in.close();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

案例：

QQ二度人脉推荐系统，资料见图：fof.jpg 和文档：qq.txt

- 一度直接好友关系 ：1
- 二度间接好友关系：2

用户         好友列表
----------------------------------------------	 
```xml
tom	         cat	hadoop	hello
hello		 mr	tom	world	hive
cat	    	 tom	hive
hive	 	 cat	hadoop	world	hello	mr
mr	     	 hive	hello
hadoop	 	 tom	hive	world
world	 	 hadoop	hive	hello
```

找到所有的直接好友关系-剔除，找出间接2度好友关系（可能包含一度好友关系 也应该剔除！）

Mapper: （K,V） 

```xml
(hello-mr,0)
(hello-tom,0)
(hello-world,0)
(hello-hive,0)
(hive-hello,1)
(mr-tom,1)   
(mr-world,1)
(mr-hive,1)
(mr-hive,0)
(mr-hello,0)
(world-hive,1)
(tom-world,1)
(tom-hive,1)
```

Reducer；  

留下纯粹的二度好友关系列表

```xml
(mr-tom,1)   
(mr-world,1)
(tom-world,1)
(tom-hive,1)
(world-hive,1)
```


最终结果：

```xml
cat hadoop 2
cat world 1
-----cat hello 2
hadoop hello 3
hadoop cat 2
hadoop mr 1
hello hadoop 3
hello cat 2
hive tom 3
```




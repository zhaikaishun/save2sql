# MYSQL和SQLSERVER数据导入简单工具

## 1. 简介
项目灵感来源于自己经常会分析一些小文本文件，经常用excel等分析不太方便，所以做了这个工具。能将文件简单导入到mysql，sqlserver的工具，目前支持文件，文件夹，hdfs文件，文件夹，以后会支持excel等文件，导入的
字段名称为v0,v1,v2...，属性全为varchar(255) 表名为指定或者默认的文件夹（或者文件名）和时间的组合  
优势在于：
1. 相比自带的工具比较方便（前提学习使用还是需要的）  
2. 能容错，某几行出错也不影响整体导入，出错的几行也能部分导入

## 2. 使用环境要求  
1. jdk1.8必须，如只有1.7需要自己编译
2. 所导入的mysql版本小于8  
3. 支持大多数sqlserver版本

## 3. 使用方法 
### 3.1 编译打包或者下载 
maven编译打包,或者使用idea打开，在idea内使用,或者在jar备份中下载（不保证最新）  
### 3.2  配置数据库相关信息
将jar包内的配置文件parameter.conf取出，需配置如下配置让后放回到jar包  
```
fs.defaultFS=192.xx.xx.xx:9000
ip=192.xx.xx.xx
user=xxx
passwd=xxx
port=3306
dbName=xxx
sqlserverOrMysql=sqlserver
```
其中  
- **fs.defaultFs**：是hdfs的web开发路径，如果想将hdfs的数据导入到数据库，需要配置这个参数
- **ip**：这是数据库的实例地址，如果有实例名，需要加两个反斜杠，例如 localhost\\\SQLEXPRESS
- **user**：数据库的用户名
- **passwd**：数据库密码
- **port**：数据库端口号
- **dbName**：数据库名,数据库名不要带中括号[]
- **sqlserverOrMysql**：填写sqlserver或者mysql，表示分别入到sqlserver还是mysql中

## 3.3 **运行命令：**  
```aidl
java -jar jar包全路径 -参数
```
其中参数有如下几种  
- **-inputPath** 必填，数据的路径，可以是文件，也可以是文件名，或者是hdfs路径  
- **-sourceType** 选填，默认是text，可解析普通的文件，gz文件。 或者填写hdfs，表示读取的是hdfs上的数据
- **-separator** 选填，程序会通过文件的前两行自动判断分隔符  
- **tableName** 选填，我们可以给表指定一个名称，若不填写，表名默认是文件或者文件夹名称+时间组成的字符串
**运行举例**  
```aidl
java -jar jar包全路径 -inputPath F:\ccf

```
上述命令会将F:\ccf文件夹下的所有文件读取，并且保存到数据库中，表明由时间和文件夹名称决定，叫做ccf_MMddHHmmss

## 4. 日志查看：  
1. 控制台会输出相关日志  
2. 当前文件夹下，logs目录也会有日志记录

## 5. 使用到的第三方工具或者框架  
1. hadoop，hdfs相关依赖  
2. args4j 解析参数  
3. mysql，sqlserver的jdbc相关依赖  
4. log4j2  

### 6. 鸣谢
shanfeng，指导了数据入库，性能得到很大提升
caohao


## 将来会做
1. excel解析入库（一定）  
2. 对mysql8版本的支持（一定）
3. 可能会做一些按照模板表入库（可能性很小，因为这个工具的目的就是做一个简单的入库小工具）



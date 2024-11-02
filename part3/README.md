<center><div style='height:4mm;'></div><div style="font-family:华文楷体;font-size:25pt;"><b>金融大数据处理技术 实验二（任务三：用户活跃度分析）</b></div></center>

<center><div style='height:2mm;'></div><div style="font-family:华文楷体;font-size:14pt;"><b>221275010 屈航</b></div></center>

# 1、设计思路

## 1.1、 Mapper的设计思路

**`Mapper`重构了一个`map`函数。**

`map`函数主要实现了对`user_balance_table.csv`表中的每一个用户的活跃天数进行统计，有直接购买（ `direct_purchase_amt`  字段大于 0 ）或赎回⾏为（  `total_redeem_amt`字段大于 0 ）时，则该⽤户当天活跃。实现的过程就是对满足条件`directPurchaseAmt.compareTo(BigInteger.ZERO) > 0 || totalRedeemAmt.compareTo(BigInteger.ZERO) > 0`的条目进行键值对的写入`context.write(userId, one)`表示找到该用户有一天活跃；否则就写入`context.write(userId, new LongWritable(0))`表示该用户在这一天不活跃。

因为考虑到求和得到的数据量可能很大，导致会超出`int`类型的表示范围，故实现的过程中的资金流入流出量的数据类型都是**`BigInteger`**。

因为要传输两个值，即每⽇的`total_purchase_amt`资⾦流⼊与 `total_redeem_amt`资金流出，故可以把这两个数据作为一个**`BalanceWritable`类**来作为`value`进行键值对的传输。

以下是**`BalanceWritable`类**的部分构造语句：

```java
public static class BalanceWritable implements Writable {
        private BigInteger inflow;
        private BigInteger outflow;
    
        public BalanceWritable() {
            this.inflow = BigInteger.ZERO;
            this.outflow = BigInteger.ZERO;
        }
    
        public void set(BigInteger inflow, BigInteger outflow) {
            this.inflow = inflow;
            this.outflow = outflow;
        }
    	@Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(inflow.toString());
            out.writeUTF(outflow.toString());
        }
    
        @Override
        public void readFields(DataInput in) throws IOException {
            inflow = new BigInteger(in.readUTF());
            outflow = new BigInteger(in.readUTF());
        }
```

以下是**`map`函数**的主要功能语句：

```java
String userIdField = fields[0];
BigInteger directPurchaseAmt = new BigInteger(fields[5]);
BigInteger totalRedeemAmt = new BigInteger(fields[8]);

userId.set(userIdField);
if (directPurchaseAmt.compareTo(BigInteger.ZERO) > 0 || totalRedeemAmt.compareTo(BigInteger.ZERO) > 0) {
    context.write(userId, one);
} else {
    context.write(userId, new LongWritable(0)); // Count as active day even if amounts are zero
}
```

*<u>**注意：实验开始前要对 `user_balance_table.csv`中的第一行删去，第一行并不是需要统计的内容。**</u>*

## 1.2、Reducer的设计思路

**`Reducer`包含有`reduce`函数和`cleanup`函数。**

1. **`reduce`函数**就是实现对于同一个key的`Iterable<LongWritable> values`中的每一个元素按照`userID`作为键，对于活跃天数进行求和操作。最后把统计完成的键值对，存入**`Map<Long, List<Text>> activeDaysMap = new TreeMap<>(Comparator.reverseOrder())`**，这个`activeDaysMap`变量存入的元素是一个键值对为（**活跃天数，userID列表**），可以实现按照`key`即**活跃天数**进行**倒序排列**，以待`cleanup`函数实现对输出格式的处理。
   以下是`reduce`函数的主要功能语句：

   ```java
   private LongWritable result = new LongWritable();
   private Map<Long, List<Text>> activeDaysMap = new TreeMap<>(Comparator.reverseOrder());
   
   @Override
   protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
       long activeDays = 0;
       for (LongWritable val : values) {
           activeDays += val.get();
       }
       result.set(activeDays);
   
       activeDaysMap.computeIfAbsent(activeDays, k -> new ArrayList<>()).add(new Text(key));
   }
   ```

2. **`cleanup`函数**就是实现对输出结果的格式控制。对于`reduce`函数中实现的**键**为**活跃天数**，**值**为**userID列表**，这已经按照活跃天数**完成**了**倒序排序**了，所以只需要把键值对的userID列表展开依次输出即可。
   以下是**`cleanup`函数**的主要功能语句：

   ```java
   for (Map.Entry<Long, List<Text>> entry : activeDaysMap.entrySet()) {
       for (Text userId : entry.getValue()) {
           context.write(userId, new LongWritable(entry.getKey()));
       }
   }
   ```

## 1.3、项目运行的配置设计

- 此次项目主要使用**`Maven`**进行项目管理，通过编辑**`pom.xml`**文件对该项目进行配置。`pom.xml`文件的配置信息包含有该项目需要哪些库文件需要下载，该项目的项目文件有哪些。


- 依次使用`mvn clean install`进行配置，同时还可以使用`mvn compile`对`.class`文件进行生成，`mvn package`实现对项目文件的`.class`文件打包成`jar`文件。


- 将`user_balance_table.csv`上传至**HDFS**的`/input`文件夹里面，最后运行该项目的`jar`文件，运行命令为：


```bash
./hadoop jar /home/njucs/shiyan2_3/target/shiyan2_3-1.0-SNAPSHOT.jar ActiveDaysCount /input /output_3
```

<u>***注意要把导出来的 part-r-00000解锁，以实现普通用户可以打开，命令如下：***</u>

```bash
sudo chown $USER part-r-00000
```

# 2、程序运行结果

以下即为**`ActiveDaysCount.java`**程序执行的任务三（根据 `user_balance_table.csv`  表中的数据，统计每个⽤户的活跃天数，并按照活跃天数降序排列。输出格式为" < 用户 ID> TAB < 活跃天数 >“）的运行结果：

- **程序运行结果图：**

<img src="screenshot\1.png" style="zoom:50%;" />

<img src="screenshot\2.png" style="zoom:50%;" />

<img src="screenshot\3.png" style="zoom:50%;" />

- **part-r-00000输出结果图：**

<img src="screenshot\4.png" style="zoom:50%;" />

# 3、WEB页面截图

因为我的代码一开始写的有bug，所有修改了4次，也就运行了5次，第5次程序运行结果符合预期，任务三完成。

<img src="screenshot\5.png" style="zoom:50%;" />




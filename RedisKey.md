获取所有键
>keys *

删除键
>del keyName

#### stream类型数据操作-独立消息
添加消息
>xadd [key] * [属性名] [属性值]

代表服务器自动生成id，也可以在*处指定id，但需要符合规则
*：添加成功返回服务器自动生成的id，为队列id-队列内id
指定id：必须比当前最后一个id大（队列id或队列内id）；

查询消息
>xrange [key] - + 

查询所有消息，-表示最小的id/最先add，+表示最大，可以查询某id之前或之后的消息
查询的返回结构如下
> -消息序号(小号为最先添加)</br>
> -消息ID</br>
> --属性名1</br>
> --属性值1</br>
> --属性名2</br>
> --属性值2</br>

删除消息

>xdel [key] [id]

阻塞消息
>xread block 0 count 1 streams [key] $

此时窗口被堵塞，无法输入其他指令；</br>
此时新打开一个窗口add消息/通过其他方式add消息，发现阻塞的窗口打印最新的消息及等待时间，并解除阻塞

#### stream-消费者组
创建消息队列及消费者组
>xgroup create [mqName消息队列名] [groupName消息组名] [0起始id] [mkstream不存在mqname时则必填，用于创建消息队列]

删除指定消费者组
>xgroup destroy maName groupName

给消费者组添加消费者
>info sever
* 查询版本后发现redis版本为5.0.4，无需显式创建，执行【XREADGROUP GROUP groupname consumername COUNT 1 STREAMS mqname >】即可读取下一条未消费的消息，前提是消息队列中需要有消息存在【xadd [key] * [属性名] [属性值]】，否则无法创建成功

查看消息队列信息，返回消费者组中的组及消费者信息
>XINFO GROUPS mqname

查询消费者信息
>XINFO CONSUMERS mqname groupname1

查询队列当前的消息
>xrange mq - +

获取key的类型
>type keyName



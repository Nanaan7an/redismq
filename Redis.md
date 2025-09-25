# Redis的安装和使用 
## 安装(CentOS为例)
1. 使用`root`权限登录，登陆方式如下
   1. **Xshell**：尝试连接22端口，连接成功则ECS运行正常。
   2. **远程连接**：阿里云管理台
2. 在`/opt`中放入`redis.tar.gz`的包
   1. **目录问题**：也可以放到别的目录中，如`/temp`等，之后解压的时候可以选择解压的目标目录
   2. **获取压缩包**:
      1. 从官网直接下载到该目录
      2. 使用`Xftp`直接从本地拉进去
3. 检查`redis`运行环境（C语言环境），没有则需要安装》gcc -v
4. 解压》tar zxf [压缩包名称，如redis.tar.gz]
   1. 解压到指定目录下》tar -zxf redis.tar.gz -C [指定目录，如/opt]
5. cd到解压文件内，编译》make
6. 编译成功后输入》make install
7. 任意目录下执行，启动成功则安装成功（此方式为**前台方式**启动，关闭窗口时进程会被终止）》redis-server


## 启动
**守护进程方式**
1. 将配置文件redis.conf复制到`/etc`中》cp /opt/redis-5.0.4/redis.conf /etc/redis/redis.conf
2. 编辑etc中的配置文件》vim redis.conf
3. 使用vim内置的搜索，使用“n”查找下一处》/daemonize
4. 使用键盘移动到daemonize no，进入编辑模式》a
5. 将`no`改为`yes`
6. ECS退出编辑模式并保存》ECS》:wq
7. 》cd usr/local/bin
8. 指定配置文件，启动服务》redis-server /etc/redis/redis.conf
9. 检查进程：ps -ef | grep redis （ps -ef：查询当前进程，且以-e所有进程和-f完整格式；|管道，把前面命令的输出作为后面命令的输入；grep：查询关键词）

## 修改redis.conf
（使用/搜索关键字时，必须在非编辑模式下[使用ESC/a切换编辑模式]）
1. 注释掉bind ，不在是本机访问》#bind 127.0.0.1
2. 将本机访问保护模式改为no》protected-mode no
3. 修改密码》requirepass  Admin@123
4. 修改日志目录，默认为根目录下的路径，需要手动建文件夹，否则启动时会报找不到路径》

## 访问远程redis
1. 在命令行cmd中cd到redis的目录/或者直接在redis目录中，在文件路径框中输入cmd
2. 连接远程redis
   </br>`redis-cli -h [ip:redis服务器IP] -p [port:一般默认为6379]`
   </br>出现【ip:端口】表示已经连接到服务器
3. 登录redis
   </br>`auth redis密码`
   </br>出现【OK】表示成功
4. 验证连通性
   </br>`ping`
   </br>出现【PONG】表示成功



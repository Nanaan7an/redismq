## 阿里云ECS的使用方法
### 重置密码
https://www.aliyun.com
控制台
左上角菜单进入“云服务器ECS”
点击实例ID
右上角“全部操作”->“修改实例密码”

### 安全组：虚拟防火墙
在实例中，点击安全组
管理规则
添加

### 防火墙
设置安全组后，需要检查一下ECS的防火墙开启的端口，可能安全组的端口并没有同步过来，需要再单独设置一下
>sudo firewall-cmd --list-all

增加监听端口
>sudo firewall-cmd --zone=public --add-port=6379/tcp --permanent

设置完成后，需要reload一下firewall才能看到是否新增端口成功
>sudo firewall-cmd --reload
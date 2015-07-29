# skipdbv2
rewrite and fix for https://github.com/stevedekorte/skipdb

实现功能有：

* dbus set key=value
* dbus ram key=value
* dbus replace key=value
* dbus get key
* dbus list key
* dbus delay key tick path_of_shell.sh
* dbus time key H:M:S path_of_shell.sh
* dbus export key #将配置导入到脚本
* dbus update key #将脚本配置保存到数据库
* dbus inc key=value #增加数值
* dbus desc key=value #减去数值
* dbus event name path_of_shell.sh #注册一个事件脚本
* dbus fire name #触发一个事件脚本

# 编译方法
* 依赖库 libev，需要自行编译或安装
* cd skipdb
* mkdir build
* cd build
* cmake ..
* make

# 执行方法
* skipd -d /path/of/data
* dbus command params

# skipdbv2
rewrite and fix for https://github.com/stevedekorte/skipdb

实现功能有：

* dbus set key value 
* dbus ram key value
* dbus replace key value 
* dbus get key 
* dbus list key 
* dbus delay key tick path_of_shell.sh
* dbus time key H:M:S path_of_shell.sh

# 编译方法
* cd skipdb
* mkdir build
* cd build
* cmake ..
* make

# 执行方法
* skipd -d /path/of/data
* dbus command params

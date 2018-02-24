# Tools
jingyun tools

## 0.9.3
DAGWorker update function find_loop. old find_loop move to find_loop2

## 0.9.1
修改不适合python3的代码，使得代码既符合python3又符合python2
DAGWorker 增加方法exist_loop find_loop 判断是否有回路和获得回路

## 0.8.11
DB execute_select 加入参数prefix_value 支持按前缀查找

## 0.8.10
test 时默认进入debug模式
执行完成后，由原先的只返回task_output该为返回task_status,task_output

## 0.8.9
fix bug: hang_up_clock test模式下sleep死循环

## 0.8.7
开发JYTools中JYWorker交互式生成配置文件

## 0.8.6
task信息全部转成unicode

## 0.8.5
fix bug: test方法return时AttributeError: 'NoneType' object has no attribute 'task_output'

## 0.8.4
fix bug: hang_up_clock debug模式下sleep死循环

## 0.8.3
fix bug: 解决task_log中publish_message时task_key中文问题

## 0.8.2
fix bug


## 0.7.11
RedisWorkerConfig类增加静态方法write_config,可以根据参数生成配置文件

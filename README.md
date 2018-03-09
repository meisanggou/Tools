# Tools
jingyun tools

## 1.0.6
ReadWorkerLog 查询的level为小写时自动转为大写

## 1.0.5
set_output的日志级别由INFO变为DEBUG

## 1.0.4
StringTools 添加方法 join_decode join_encode
JYWorker添加类ReadWorkerLog

## 1.0.3
RedisWorker 添加方法 has_task_item

## 1.0.2
DAGWorker 引用支持key以数字开头
StringTools 添加方法 m_print
fix JYWorker task log bug in debug module

## 1.0.1
JYWorker添加方法sub_classes，获得所有最后的RedisClass的子类

## 0.9.11
worker运行时 增加打印clock_key

## 0.9.9
只要params是dict类型都转换成WorkerTaskParams类型

## 0.9.8
RedisStat 增加 list_queue_detail
fix DAGWorker bug

## 0.9.7
RedisStat 增加 list_worker list_worker_detail

## 0.9.5
增加requires six

## 0.9.4
fix find_loop bug

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

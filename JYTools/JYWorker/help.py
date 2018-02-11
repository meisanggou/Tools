#! /usr/bin/env python
# coding: utf-8
import sys
from JYTools.JYWorker._config import RedisWorkerConfig, WorkerConfig
if sys.version_info[0] == 3:
    raw_input = input

"""
add in version 0.8.7
"""
redis_host_input = "please input the redis_host, need the format like 192.168.0.1, default localhost"
redis_password_input = "please input the redis_password, need string redis_password, default null"
redis_port_input = "please input the redis_port, need number redis_port < 65535, default 6379"
redis_db_input = "please input the redis_db, need number redis_db <= 16, default 13"
select_key_input = "if you want to change the value, input the number ahead, or you can input w to finish:"
heartbeat_prefix_key_input = "please input the heartbeat_prefix_key, need string heartbeat_prefix_key, " \
                             "default worker_heartbeat"
queue_prefix_key_input = "please input the queue_prefix_key, need string queue_prefix_key, default task_queue"
judge_prefix_input = "either the queue_prefix_key or the heartbeat_prefix_key cannot contain the other, input again"
pop_time_out_input = "please input the pop_time_out, need number pop_time_out, default 60"
file_path_input = "please input the config file path, need string config file path ,default redis.conf:"
previous_set = "Previously set:"


def create_config():
    file_path = raw_input("%s" % file_path_input)
    if not file_path.strip():
        file_path = 'redis.conf'
    redis_host = raw_input("%s:" % redis_host_input)
    num = redis_host.split('.')
    len_num = len(num)
    if not redis_host.strip() or redis_host == "localhost":
        redis_host = 'localhost'
    else:
        while len_num != 4:
            redis_host = raw_input("%s:" % redis_host_input)
            if not redis_host.strip() or redis_host == "localhost":
                redis_host = 'localhost'
                break
            else:
                num = redis_host.split('.')
                len_num = len(num)
        while len_num == 4 and not (num[0].isdigit() and num[1].isdigit() and num[2].isdigit() and num[3].isdigit()):
            redis_host = raw_input("%s:" % redis_host_input)
            if not redis_host.strip() or redis_host == "localhost":
                redis_host = 'localhost'
            else:
                num = redis_host.split('.')
                len_num = len(num)
                while len_num != 4:
                    redis_host = raw_input("%s:" % redis_host_input)
                    if not redis_host.strip() or redis_host == "localhost":
                        redis_host = 'localhost'
                        break
                    else:
                        num = redis_host.split('.')
                        len_num = len(num)
    redis_password = raw_input("%s:" % redis_password_input)
    if not redis_password.strip():
        redis_password = ''
    redis_port = raw_input("%s:" % redis_port_input)
    if not redis_port.strip():
        redis_port = '6379'
    else:
        while not redis_port.isdigit() or int(redis_port) >= 65535 or int(redis_port) <= 0:
            redis_port = raw_input("%s:" % redis_port_input)
            if not redis_port.strip():
                redis_port = '6379'
    redis_db = raw_input("%s:" % redis_db_input)
    if not redis_db.strip():
        redis_db = '13'
    else:
        while not redis_db.isdigit() or int(redis_db) > 16 or int(redis_db) <= 0:
            redis_db = raw_input("%s:" % redis_db_input)
            if not redis_db.strip():
                redis_db = '13'
    heartbeat_prefix_key = raw_input("%s:" % heartbeat_prefix_key_input)
    if not heartbeat_prefix_key.strip():
        heartbeat_prefix_key = 'worker_heartbeat'
    queue_prefix_key = raw_input("%s:" % queue_prefix_key_input)
    if not queue_prefix_key.strip():
        queue_prefix_key = 'task_queue'
    while queue_prefix_key in heartbeat_prefix_key or heartbeat_prefix_key in queue_prefix_key:
        queue_prefix_key = raw_input("%s:" % judge_prefix_input)
    pop_time_out = raw_input("%s:" % pop_time_out_input)
    if not pop_time_out.strip():
        pop_time_out = '60'
    else:
        while not pop_time_out.isdigit():
            pop_time_out = raw_input("%s:" % pop_time_out_input)
            if not pop_time_out.strip():
                pop_time_out = '60'
    select = True
    while select:
        print ("1 redis_host: %s\n2 redis_password: %s\n3 redis_port: %s\n4 redis_db:%s\n"
               "5 heartbeat_prefix_key: %s\n6 queue_prefix_key: %s\n7 pop_time_out: %s" %
               (redis_host, redis_password, redis_port, redis_db, heartbeat_prefix_key, queue_prefix_key, pop_time_out))
        select_key = raw_input("%s\n" % select_key_input)
        if select_key == '1':
            redis_host_last = redis_host
            redis_host = raw_input("%s, %s %s):" % (redis_host_input, previous_set, redis_host_last))
            num = redis_host.split('.')
            len_num = len(num)
            if not redis_host.strip():
                redis_host = redis_host_last
            else:
                while len_num != 4:
                    redis_host = raw_input("%s, %s %s):" % (redis_host_input, previous_set, redis_host_last))
                    if not redis_host.strip():
                        redis_host = redis_host_last
                        break
                    else:
                        num = redis_host.split('.')
                        len_num = len(num)
                while len_num == 4 and not \
                        (num[0].isdigit() and num[1].isdigit() and num[2].isdigit() and num[3].isdigit()):
                    redis_host = raw_input("%s, %s %s):" % (redis_host_input, previous_set, redis_host_last))
                    if not redis_host.strip():
                        redis_host = redis_host_last
                        break
                    else:
                        num = redis_host.split('.')
                        len_num = len(num)
                        while len_num != 4:
                            redis_host = raw_input("%s, %s %s):" % (redis_host_input, previous_set, redis_host_last))
                            if not redis_host.strip():
                                redis_host = redis_host_last
                                break
                            else:
                                num = redis_host.split('.')
                                len_num = len(num)
        if select_key == '2':
            redis_password_last = redis_password
            redis_password = raw_input("%s, %s %s:" % (redis_password_input, previous_set, redis_password_last))
            if not redis_password.strip():
                redis_password = redis_password_last
        if select_key == '3':
            redis_port_last = redis_port
            redis_port = raw_input("%s, %s %s:" % (redis_port_input, previous_set, redis_port_last))
            if not redis_port.strip():
                redis_port = redis_port_last
            else:
                while not redis_port.isdigit() or int(redis_port) >= 65535 or int(redis_port) <= 0:
                    redis_port = raw_input("%s, %s %s:" % (redis_port_input, previous_set, redis_port_last))
                    if not redis_port.strip():
                        redis_port = redis_port_last
        if select_key == '4':
            redis_db_last = redis_db
            redis_db = raw_input("%s, %s %s):" % (redis_db_input, previous_set, redis_db_last))
            if not redis_db.strip():
                redis_db = '13'
            else:
                while not redis_db.isdigit() or int(redis_db) > 16 or int(redis_db) <= 0:
                    redis_db = raw_input("%s, %s %s):" % (redis_db_input, previous_set, redis_db_last))
                    if not redis_db.strip():
                        redis_db = redis_db_last
        if select_key == '5':
            heartbeat_prefix_key_last = heartbeat_prefix_key
            heartbeat_prefix_key = raw_input("%s, %s %s):" % (heartbeat_prefix_key_input, previous_set,
                                                              heartbeat_prefix_key_last))
            if not heartbeat_prefix_key.strip():
                heartbeat_prefix_key = heartbeat_prefix_key_last
            while queue_prefix_key in heartbeat_prefix_key or heartbeat_prefix_key in queue_prefix_key:
                heartbeat_prefix_key = raw_input("%s, %s %s):" % (judge_prefix_input, previous_set,
                                                                  heartbeat_prefix_key_last))
        if select_key == '6':
            queue_prefix_key_last = queue_prefix_key
            queue_prefix_key = raw_input("%s, %s %s):" % (queue_prefix_key_input, previous_set, queue_prefix_key_last))
            if not queue_prefix_key.strip():
                queue_prefix_key = queue_prefix_key_last
            while queue_prefix_key in heartbeat_prefix_key or heartbeat_prefix_key in queue_prefix_key:
                queue_prefix_key = raw_input("%s:" % judge_prefix_input)
        if select_key == '7':
            pop_time_out_last = pop_time_out
            pop_time_out = raw_input("%s, %s %s):" % (pop_time_out_input, previous_set, pop_time_out_last))
            if not pop_time_out.strip():
                pop_time_out = pop_time_out_last
            else:
                while not pop_time_out.isdigit():
                    pop_time_out = raw_input("%s, %s %s):" % (pop_time_out_input, previous_set, pop_time_out_last))
                    if not pop_time_out.strip():
                        pop_time_out = pop_time_out_last
        if not select_key.strip() or select_key == 'w':
            select = False
    config = dict()
    config["file_path"] = file_path
    config["redis_host"] = redis_host
    config["redis_password"] = redis_password
    config["redis_port"] = redis_port
    config["redis_db"] = redis_db
    config["heartbeat_prefix_key"] = heartbeat_prefix_key
    config["queue_prefix_key"] = queue_prefix_key
    config["pop_time_out"] = pop_time_out
    print ("the config file path: %s\n1 redis_host: %s\n2 redis_password: %s\n3 redis_port: %s\n"
           "4 redis_db:%s\n5 heartbeat_prefix_key:%s\n6 queue_prefix_key:%s\n7 pop_time_out:%s\n" %
           (file_path, redis_host, redis_password, redis_port, redis_db, heartbeat_prefix_key, queue_prefix_key,
            pop_time_out))
    return config

if __name__ == "__main__":
    config_result = create_config()
    RedisWorkerConfig.write_config(config_result["file_path"], config_result["redis_host"],
                                   config_result["redis_password"], config_result["redis_port"],
                                   config_result["redis_db"], section_name="Redis", append=False)
    WorkerConfig.write_work_config(config_result["file_path"], config_result["heartbeat_prefix_key"],
                                   config_result["queue_prefix_key"], config_result["pop_time_out"],
                                   section_name="Worker", append=True)


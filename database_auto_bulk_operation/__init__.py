# coding=utf8
"""
@author:Administrator
@file: bulk_operation.py
@time: 2018/08/27

强大的自动批量聚合操作各种数据库，不需要再调用处手动去喂给批量调用方法一个个组装好了数组。
自动批量聚合操作数据库能减少客户端和数据库服务端的io往返次数，效率提升大。

包括支持mysql mongo redis elastic。


主要原理是批量操作对象内部有一个while 1的守护线程，不断的去自动组合列表任务，然后调用各种数据库的的python包的批量操作方法。
同时加入一个atexit的钩子，防止守护线程随程序一起结束时候，掉一批还未批量插入的尾部任务。


各种数据库的更简单的批次操作，主要是不用再调用处手动切割分组来调用原生中间件操作类的批量操作方法
对于未知时间的离散任务能够自动批量聚合，这种情况下无法自己提前切割分组，使用此包的方式非常适合。

"""

"""
如果不使用自动批量聚合会造成到处重复写很多切割分组的代码，思维不缜密的对非整除批次的分组还会漏掉最后一部分余数的批次

举个真实例子，以下是实现每批次插入1000个到mongo，自己要维护一个数组变量mongo_list，
然后每次插入后清除数组，为了防止非整除，还要再在for循环以外插入一次。

临时写一次两次还好，一个项目中写个几十次这样的类似重复逻辑场景的代码，就要吐血了。
写代码的出发点是宁愿十分困难的搞一次，也不愿意不困难但是重复的麻烦几百次。

mongo_list = []
for p_id, value in place_brand_dict.items():
    if not hotel_city.find_one({'_id': p_id}):
        continue
    val_list = []
    for brand_item in value.keys():
        value[brand_item]['star'] = star_dict[brand_item]['star']
        val_list.append(value[brand_item])
    mongo_list.append(UpdateOne({'_id': p_id}, {'$set': {'brands': val_list, 'place_id': p_id}}, upsert=True))
    if len(mongo_list) == 1000:
        city_brand_new.bulk_write(mongo_list)
        mongo_list = []
if mongo_list:
    city_brand_new.bulk_write(mongo_list)
"""

"""
## 上面这种代码是固定的从一个已知确定的字典中，把任务写到mongo，这样实现不难，但对未来未知时间的离散任务要批量插入就难了。

```
例如是你写了一个函数是插入mongo记录的，这个函数是被供给被别的地方调用，并不知道会什么时候调用和调用多少次你的mongo插入函数
这种情况下做数组预切割，然后调用pymongo的bulk_write方法，那就不可能实现了。

对于这种场景是最适合使用此包的自动批量聚合功能的。
```
"""

import atexit
from typing import Union, Tuple
import abc
import time
from queue import Queue, Empty
import unittest
# noinspection PyUnresolvedReferences
from pymongo import UpdateOne, InsertOne, UpdateMany, collection, MongoClient
from elasticsearch import helpers, Elasticsearch
import redis
import torndb_for_python3
from nb_log import LoggerMixin
from decorator_libs import keep_circulating, TimerContextManager


class RedisOperation:
    """redis的操作，此类作用主要是规范下格式而已"""

    def __init__(self, redis_operation_mehtod_name: str, *args, **kwargs):
        """
        :param operation_name: Redis类的操作方法的名字，例如 sadd lpush set等
          redis.set('a',1,ex=1),  为 ('set','a',1,ex=1)
        :param key: redis的键
        :param value: reids键的值
        """
        self.redis_operation_mehtod_name = redis_operation_mehtod_name
        self.args = args
        self.kwargs = kwargs


class BaseBulkHelper(LoggerMixin, metaclass=abc.ABCMeta):
    """批量操作抽象基类"""
    bulk_helper_map = {}

    def __new__(cls, middleware_opration_python_instance, *args, **kwargs):
        if str(middleware_opration_python_instance) not in cls.bulk_helper_map:  # 加str是由于有一些类型的实例不能被hash作为字典的键
            self = super().__new__(cls)
            self._custom_init(middleware_opration_python_instance, *args, **kwargs)
            cls.bulk_helper_map[str(middleware_opration_python_instance)] = self
            return self
        else:
            return cls.bulk_helper_map[str(middleware_opration_python_instance)]

    def __init__(self, middleware_opration_python_instance: Union[
        collection.Collection, redis.Redis, Elasticsearch, Tuple[torndb_for_python3.Connection, str]],
                 threshold: int = 100, max_time_interval=10, is_print_log: bool = True):
        """
        仅仅是让pycharm能自动补全
        :param middleware_opration_python_instance: 操作数据库（中间件）的对象，例如pymongo的Collection类的实例，redis的Redis类的实例
        :param threshold:多少个任务聚合成一次操作
        :param max_time_interval:如果指定的时间间隔内没达到threashold阈值就直接插入
        :param is_print_log:
        """
        pass

    def _custom_init(self, middleware_opration_python_instance: Union[
        collection.Collection, redis.Redis, Elasticsearch, Tuple[torndb_for_python3.Connection, str]],
                     threshold: int = 100, max_time_interval=10, is_print_log: bool = True):
        self.middleware_opration_python_instance = middleware_opration_python_instance
        self._threshold = threshold
        self._max_time_interval = max_time_interval
        self._is_print_log = is_print_log
        self._to_be_done_queue = Queue(threshold)
        self._last_oprate_time = time.time()
        self._last_has_task_time = time.time()
        atexit.register(self._do_bulk_operation)  # 程序自动结束前执行注册的函数
        self.__excute_bulk_operation_in_other_thread()
        self.__check_queue_size()
        self.logger.debug(f'{self.__class__}被实例化')

    def add_task(self, base_operation: Union[UpdateOne, InsertOne, RedisOperation, tuple, dict]):
        """添加单个需要执行的操作，程序自动聚合陈批次操作"""
        self._to_be_done_queue.put(base_operation)

    @keep_circulating(1, block=False, daemon=True)  # redis异常或网络异常，使其自动恢复。
    def __excute_bulk_operation_in_other_thread(self):
        while True:
            if self._to_be_done_queue.qsize() >= self._threshold or time.time() > self._last_oprate_time + self._max_time_interval:
                self._do_bulk_operation()
            time.sleep(0.01)

    @keep_circulating(1, block=False, daemon=True)
    def __check_queue_size(self):
        if self._to_be_done_queue.qsize() > 0:
            self._last_has_task_time = time.time()
        if time.time() - self._last_has_task_time > 60:
            self.logger.info(
                f'{self.middleware_opration_python_instance} 最近一次有任务的时间是 ： {time.strftime("%Y-%m-%d %H;%M:%S", time.localtime(self._last_has_task_time))}')

    def _do_bulk_operation(self):
        if self._to_be_done_queue.qsize() > 0:
            t_start = time.time()
            count = 0
            to_be_done_list = []
            for _ in range(self._threshold):
                try:
                    request = self._to_be_done_queue.get_nowait()
                    count += 1
                    to_be_done_list.append(request)
                except Empty:
                    break
            if to_be_done_list:
                self._bulk_operate_realize(to_be_done_list)
            if self._is_print_log:
                self.logger.info(
                    f'【{self.middleware_opration_python_instance}】  批量操作的任务数量是 {count} 消耗的时间是 {round(time.time() - t_start, 6)}')
            self._last_oprate_time = time.time()

    @abc.abstractmethod
    def _bulk_operate_realize(self, to_be_done_list):
        raise NotImplementedError


class MongoBulkWriteHelper(BaseBulkHelper):
    """
    一个更简单的批量插入,可以直接提交一个操作，自动聚合多个操作为一个批次再插入，速度快了n倍。
    """

    def _bulk_operate_realize(self, to_be_done_list):
        self.middleware_opration_python_instance.bulk_write(to_be_done_list, ordered=False)


class ElasticBulkHelper(BaseBulkHelper):
    """
    elastic批量插入。
    """

    def _bulk_operate_realize(self, to_be_done_list):
        helpers.bulk(self.middleware_opration_python_instance, to_be_done_list)


class RedisBulkWriteHelper(BaseBulkHelper):
    """redis批量插入，比自带的更方便操作非整除批次"""

    def _bulk_operate_realize(self, to_be_done_list):
        pipeline = self.middleware_opration_python_instance.pipeline()  # type: redis.client.Pipeline
        for to_be_done in to_be_done_list:
            getattr(pipeline, to_be_done.redis_operation_mehtod_name)(*to_be_done.args, **to_be_done.kwargs)
        pipeline.execute()
        pipeline.reset()


class MysqlBulkWriteHelper(BaseBulkHelper):
    """mysql批量插入，比自带的更方便操作非整除批次
    """

    def _bulk_operate_realize(self, to_be_done_list):
        self.middleware_opration_python_instance[0].executemany_rowcount(self.middleware_opration_python_instance[1],
                                                                         to_be_done_list)


# noinspection SpellCheckingInspection
class _Test(unittest.TestCase, LoggerMixin):
    @unittest.skip
    def test_mongo_bulk_write(self):
        # col = MongoMixin().mongo_16_client.get_database('tests').get_collection('ydf_test2')
        col = MongoClient().get_database('tests').get_collection('ydf_test2')
        with TimerContextManager():
            for i in range(50000 + 13):
                # time.sleep(0.01)
                item = {'_id': i, 'field1': i * 2}
                mongo_helper = MongoBulkWriteHelper(col, 10000, is_print_log=True)
                mongo_helper.add_task(UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True))

    # @unittest.skip
    def test_redis_bulk_write(self):
        with TimerContextManager():
            r = redis.Redis()
            redis_helper = RedisBulkWriteHelper(r, 2000)
            # redis_helper = RedisBulkWriteHelper(r, 100)  # 放在外面可以
            for i in range(100003):
                # time.sleep(0.2)
                # 也可以在这里无限实例化
                redis_helper = RedisBulkWriteHelper(r, 2000)
                redis_helper.add_task(RedisOperation('sadd', 'key_set', str(i)))
                redis_helper.add_task(RedisOperation('lpush', 'key_list', str(i)))

    @unittest.skip
    # noinspection PyMethodMayBeStatic
    def test_mysql_bulk_write(self):
        # noinspection PyArgumentEqualDefault
        mysql_conn = torndb_for_python3.Connection(host='localhost', database='tests', user='root', password='123456',
                                                   charset='utf8')
        with TimerContextManager():
            for i in range(100000 + 9):
                mysql_helper = MysqlBulkWriteHelper(
                    (mysql_conn, 'INSERT INTO tests.table_2 (column_1, column_2) VALUES (%s,%s)'),
                    threshold=2000, )  # 支持无限实例化
                mysql_helper.add_task((i, i * 2))


if __name__ == '__main__':
    unittest.main()

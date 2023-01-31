from __future__ import absolute_import
from six.moves import range
from six.moves import zip
__author__ = 'jasonl'

import pickle
import logging
from time import time
import base64
import uuid
import boto3
from houzz.common.module.env_module import EnvModule

from thrift.protocol import TJSONProtocol
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

def is_base64_encoded(str):
    return str and str[0] != ' ' and str[0] != '{'

def thrift_binary_serialize(thrift_obj):
    transport_out = TTransport.TMemoryBuffer()
    protocol_out = TBinaryProtocol.TBinaryProtocol(transport_out)
    thrift_obj.write(protocol_out)
    return base64.b64encode(transport_out.getvalue())


def thrift_binary_deserialize(thrift_bytes, thrift_class):
    thrift_bytes = base64.b64decode(thrift_bytes)
    transport_in = TTransport.TMemoryBuffer(thrift_bytes)
    protocol_in = TBinaryProtocol.TBinaryProtocol(transport_in)
    thrift_obj = thrift_class()
    thrift_obj.read(protocol_in)
    return thrift_obj

def thrift_json_serialize(thrift_obj):
    transport_out = TTransport.TMemoryBuffer()
    protocol_out = TJSONProtocol.TJSONProtocol(transport_out)
    thrift_obj.write(protocol_out)
    return transport_out.getvalue()


def thrift_json_deserialize(thrift_bytes, thrift_class):
    transport_in = TTransport.TMemoryBuffer(thrift_bytes)
    protocol_in = TJSONProtocol.TJSONProtocol(transport_in)
    thrift_obj = thrift_class()
    thrift_obj.read(protocol_in)
    return thrift_obj

def get_bucket_name():
    return 'houzz-prod-redis-sync'

def write_to_s3(data):
    sts_client = boto3.client('sts')
    id = uuid.uuid4()
    assumedRoleObject = sts_client.assume_role(
        RoleArn="arn:aws:iam::477121734600:role/ProdRedisSyncWrite",
        RoleSessionName="session_%s" % id
    )
    credentials = assumedRoleObject['Credentials']
    s3 = boto3.resource(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )
    bucket = s3.Bucket(get_bucket_name())
    filename = "%s_%s" % (id, int(time() * 1000))
    bucket.put_object(Key=filename, Body=pickle.dumps(data))

def get_s3_files():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(get_bucket_name())
    return {obj: pickle.loads(obj.get()['Body'].read()) for obj in bucket.objects.all()}

class RedisRepo(object):

    def __init__(self, redis, namespace, thrift_class=None, ttl = None):
        self._redis = redis
        self._namespace = namespace if namespace.endswith('/') else namespace + '/'
        self._ttl = ttl
        self._thrift_class = thrift_class
        self._log = logging.getLogger(self.__class__.__name__)
        self._param = {
            'namespace': namespace,
            'thrift_class': thrift_class,
            'ttl': ttl,
        }

    def full_key(self, key):
        return self._namespace + 'data/' + key

    def key_for_grp(self):
        return self._namespace + 'grp'

    def _get_values_from_keys(self, keys):
        values = []
        for k in keys:
            v = self.get(k)
            if v:
                values.append(v)
        return values

    def _pipeline_get_values_from_keys(self, keys):
        start_time = int(time() * 1000)
        try:
            # resolve potential missing data issue
            res = []
            for i in range(0, 3):
                pipe = self._redis.pipeline(transaction=False)
                for key in keys:
                    pipe.get(self.full_key(key))
                values = pipe.execute()
                res = []
                missing_data = []
                for key, value in zip(keys, values):
                    if value is not None:
                        res.append(self.load_data(key, value))
                    else:
                        missing_data.append(self.full_key(key))
                self._log.info("PIPELINE RESULT: key size " + str(len(keys)) + " return size " + str(len(values))
                         + " result size " + str(len(res)))
                if len(res) == len(keys):
                    retrieve_records_time = int(time() * 1000) - start_time
                    self._log.info('PIPELINE GET Time to retrieve records: ' + str(retrieve_records_time))
                    return res
                else:
                    self._log.info("Missing data is: " + str(missing_data))

            return res
        except Exception as e:
            raise Exception("Pipeline error: " + str(e))

    @classmethod
    def _is_out_of_sync(cls, old_obj, obj):
        if hasattr(old_obj, 'last_modified_time') and \
            hasattr(obj, 'last_modified_time') and \
                obj.last_modified_time > 0:
                    return old_obj.last_modified_time != obj.last_modified_time
        else:
            return False

    def get_serialized_data(self, obj, update=False):
        if update or obj.last_modified_time is None:
            obj.last_modified_time = int(time() * 1000)
        if self._thrift_class:
            serialized_data = thrift_binary_serialize(obj)
        else:
            serialized_data = pickle.dumps(obj)
        return serialized_data

    def save(self, key, obj, last_modified_time=None):
        try:
            if EnvModule().is_aws():
                data = {
                    'class': RedisRepo,
                    'init_param': self._param,
                    'method': 'save',
                    'param': {
                        'key': key,
                        'obj': obj,
                    }
                }
                write_to_s3(data)
        except Exception:
            self._log.exception("failed to write data to s3")

        full_key = self.full_key(key)
        with self._redis.pipeline() as pipe:
            try:
                # pipe.watch(full_key)
                cur_obj = self.get(key)
                if cur_obj is not None and self._is_out_of_sync(cur_obj, obj):
                    raise Exception("RedisRepo Data is updated out of sync for key [%s], new obj: %s, old object: %s" % (full_key, cur_obj, obj))
                obj.last_modified_time = last_modified_time if last_modified_time else int(time()*1000)
                if self._thrift_class:
                    pickled_object = thrift_binary_serialize(obj)
                else:
                    pickled_object = pickle.dumps(obj)
                # pipe.multi()
                if self._ttl:
                    pipe.setex(full_key, pickled_object, self._ttl)
                else:
                    pipe.set(full_key, pickled_object)
                pipe.sadd(self.key_for_grp(), key)
                pipe.execute()
            except Exception:
                self._log.exception("failed to save the key %s", key)
                raise
        return obj

    def load_data(self, key, value):
        try:
            obj = pickle.loads(value)
            self._log.info("successfully loaded the obj using pickle loads for the key %s", key)
        except Exception as e:
            #self._log.exception("failed to load the value, fallback to thrift deserializer for key %s", key)
            if self._thrift_class:
                if is_base64_encoded(value):
                    obj = thrift_binary_deserialize(value, self._thrift_class)
                else:
                    obj = thrift_json_deserialize(value, self._thrift_class)
            else:
                raise e
        return obj

    def get(self, key):
        full_key = self.full_key(key)
        value = self._redis.get(full_key)
        obj = None
        # if value is None:
        #     if self._redis.sismember(self.key_for_grp(), key):
        #         self._redis.zrem(self.key_for_grp(), key)
        # else:
        #     obj = self.load_data(key, value)
        if value is not None:
            obj = self.load_data(key, value)
        return obj

    def get_all(self):
        keys = self._redis.smembers(self.key_for_grp())
        return self._pipeline_get_values_from_keys(keys)

    def get_all_keys(self):
        return self._redis.smembers(self.key_for_grp())

    def delete(self, key):
        try:
            if EnvModule().is_aws():
                data = {
                    'class': RedisRepo,
                    'init_param': self._param,
                    'method': 'delete',
                    'param': {
                        'key': key,
                    }
                }
                write_to_s3(data)
        except Exception:
            self._log.exception("failed to write data to s3")

        full_key = self.full_key(key)
        with self._redis.pipeline() as pipe:
            try:
                #pipe.watch(full_key)
                # pipe.multi()
                pipe.srem(self.key_for_grp(), key)
                pipe.delete(full_key)
                pipe.execute()
            except Exception:
                raise
                # raise Exception("Data is updated during the change, need refresh")
        return


class RedisRepoV2(object):

    def __init__(self, redis, namespace, thrift_class=None, ttl=None):
        self._redis = redis
        self._namespace = namespace if namespace.endswith('/') else namespace + '/'
        self._ttl = ttl
        self._thrift_class = thrift_class
        self._log = logging.getLogger(self.__class__.__name__)
        self._param = {
            'namespace': namespace,
            'thrift_class': thrift_class,
            'ttl': ttl,
        }

    def data_key(self, obj_id):
        return self._namespace + 'data/%s' % obj_id

    def grp_key(self):
        return self._namespace + 'grp'

    def next_id(self):
        key = self._namespace + 'next_id'
        try:
            if EnvModule().is_aws():
                data = {
                    'class': RedisRepoV2,
                    'init_param': self._param,
                    'method': 'next_id',
                    'param': {}
                }
                write_to_s3(data)
        except Exception:
            self._log.exception("failed to write data to s3")

        return self._redis.incr(key)

    def get_count(self):
        return self._redis.get(self._namespace + 'next_id')

    @classmethod
    def _is_out_of_sync(cls, old_obj, obj):
        if hasattr(old_obj, 'last_modified_time') and \
            hasattr(obj, 'last_modified_time') and \
                old_obj.last_modified_time != obj.last_modified_time:
            return True
        else:
            return False

    def save(self, obj, sort_key=None, last_modified_time=None):
        try:
            if EnvModule().is_aws():
                data = {
                    'class': RedisRepoV2,
                    'init_param': self._param,
                    'method': 'save',
                    'param': {
                        'obj': obj
                    }
                }
                write_to_s3(data)
        except Exception:
            self._log.exception("failed to write data to s3")

        obj_id = obj.id if obj.id else self.next_id()
        data_key = self.data_key(obj_id)
        with self._redis.pipeline() as pipe:
            try:
                # pipe.watch(data_key)
                cur_obj = self.get(obj_id)
                if cur_obj is not None and self._is_out_of_sync(cur_obj, obj):
                    raise Exception("RedisRepoV2 Data is updated out of sync for key [%s], new obj: %s, old object: %s" %
                                    (obj_id, cur_obj, obj))
                obj.last_modified_time = last_modified_time if last_modified_time else int(time()*1000)
                obj.id = obj_id
                if self._thrift_class:
                    pickled_object = thrift_binary_serialize(obj)
                else:
                    pickled_object = pickle.dumps(obj)
                # pipe.multi()
                if self._ttl:
                    pipe.setex(data_key, pickled_object, self._ttl)
                else:
                    pipe.set(data_key, pickled_object)
                if sort_key is None:
                    sort_key = obj.last_modified_time
                pipe.zadd(self.grp_key(), **{str(obj_id): sort_key})
                pipe.execute()
            except Exception:
                # raise Exception("Data is updated during the change, need refresh")
                raise
        return obj_id

    def get(self, obj_id):
        data_key = self.data_key(obj_id)
        value = self._redis.get(data_key)
        obj = None
        if value is None:
            self._redis.zrem(self.grp_key(), obj_id)
        else:
            obj = self._deserialize(value)
        return obj

    def get_with_no_del(self, obj_id):
        data_key = self.data_key(obj_id)
        value = self._redis.get(data_key)
        obj = None
        if not value is None:
            obj = self._deserialize(value)
        return obj

    def _deserialize(self, value):
        try:
            obj = pickle.loads(value.encode("utf-8"))
        except Exception as e:
            if self._thrift_class:
                if is_base64_encoded(value):
                    obj = thrift_binary_deserialize(value, self._thrift_class)
                else:
                    obj = thrift_json_deserialize(value, self._thrift_class)
            else:
                raise e
        return obj

    def get_serialized_data(self, obj, update=False):
        if update or obj.last_modified_time is None:
            obj.last_modified_time = int(time() * 1000)
        if self._thrift_class:
            serialized_data = thrift_binary_serialize(obj)
        else:
            serialized_data = pickle.dumps(obj)
        return serialized_data

    #highly custmized for epn
    def get_all(self, start_ts=0, end_ts=-1, start=None, num=None):
        current_ts = int(time() * 1000)
        inf_end_ts = False
        if end_ts == -1:
            end_ts = current_ts
            inf_end_ts = True
        else:
            end_ts *= 1000
        start_ts = int(start_ts * 1000)

        if self._ttl:
            expiration_ts = current_ts - self._ttl * 1000
            # delete expired keys from the group
            self._redis.zremrangebyscore(self.grp_key(), 0, expiration_ts)
            if expiration_ts > end_ts: # all keys expired
                return []
            elif expiration_ts > start_ts:
                    start_ts = expiration_ts
        if inf_end_ts:
            end_ts = '+inf'
        if start and num or (start == 0 and num):
            keys = self._redis.zrevrangebyscore(self.grp_key(), end_ts, start_ts)
            selected_keys = []
            for key in keys:
                if int(key) >= start and int(key) < start+num:
                    selected_keys.append(key)
            keys = selected_keys
        else:
            keys = self._redis.zrevrangebyscore(self.grp_key(), end_ts, start_ts, 0, 400)
        # return [self.get(k) for k in keys]

        res = []

        for i in range(0, 3):
            res = []
            missing_data = []
            pipe = self._redis.pipeline(transaction=False)
            for key in keys:
                pipe.get(self.data_key(key))
            values = pipe.execute()

            for key, value in zip(keys, values):
                if value is not None:
                    res.append(self._deserialize(value))
                else:
                    missing_data.append(key)
            if len(keys) == len(res):
                return res
            else:
                self._log.info("Missing data is " + str(missing_data))

        return res


    def delete(self, obj_id):
        try:
            if EnvModule().is_aws():
                data = {
                    'class': RedisRepoV2,
                    'init_param': self._param,
                    'method': 'delete',
                    'param': {
                        'object_id': obj_id,
                    }
                }
                write_to_s3(data)
        except Exception:
            self._log.exception("failed to write data to s3")

        data_key = self.data_key(obj_id)
        with self._redis.pipeline() as pipe:
            try:
                # pipe.watch(data_key)
                # pipe.multi()
                pipe.zrem(self.grp_key(), obj_id)
                pipe.delete(data_key)
                pipe.execute()
            except Exception:
                # raise Exception("Data is updated during the change, need refresh")
                raise
        return

package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @Author 写你的名字
 * @Date 2022/10/10 15:32 （可以根据需要修改）
 * @Version 1.0 （版本号）
 */
@Component
@Slf4j
public class CacheClient {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                          Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否是空值, 这里必须要进行判断，因为我的redis的值为空的话，那么
        // 上面这个if是进不去的
        if (json != null) {
            // 上面已经判断过有值的情况了，所以如果程序走到了这里，只要不为null，
            // 那么就是空字符串
            // 如果是空字符串的话那就直接返回错误信息
            return null;
        }
        // 4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", time, unit);
            return null;
        }
        // 6.存在，写入redis
        this.set(key, r, time, unit);
        // 7.返回
        return r;
    }


    public <R, ID> R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 如果不存在直接返回null
            return null;
        }
        // 4. 命中，需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5. 判断是否过期
        // 如果当前过时间在当前时间的后面，就代表还没有过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 还没有过期，直接返回店铺
            return r;
        }
        // 5.2. 已过期，需要缓存重建
        // 6. 缓存重建
        // 6.1. 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2. 判断是否获取锁成功
        if (isLock) {
            // 6.3. 成功, 开启独立线程，实现缓存重建
            // 但是在实现缓存重建之前，我们需要再次检测redis缓存是否过期
            // 做DoubleCheck，如果存在则无序重建缓存
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            expireTime = redisData.getExpireTime();
            if (expireTime.isAfter(LocalDateTime.now())) {
                return r;
            }
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库, 那么查询数据库的时候就又出现问题了
                    // 我根本不知道你的数据库应该怎么去查, 所以
                    // 这一段业务逻辑应该是用户传给我们的
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        // 6.4. 返回的商铺信息
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flg = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 这里我们应该使用hutool的这个工具包，因为我的返回类型是boolean，但是我这个
        // flg的类型是Boolean，是一个类，里面涉及到null的问题，所以用这个工具包我们
        // 可以轻松应对
        return BooleanUtil.isTrue(flg);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        // 判断命中的是否是空值, 这里必须要进行判断，因为我的redis的值为空的话，那么
        // 上面这个if是进不去的
        if (json != null) {
            // 上面已经判断过有值的情况了，所以如果程序走到了这里，只要不为null，
            // 那么就是空字符串
            // 如果是空字符串的话那就直接返回错误信息
            return null;
        }
        // 4开始实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2 判断是否获取成功
            if (!isLock) {
                // 4.3 失败，则休眠并且重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit); // 做递归，一直尝试
            }
            // 如果代码走到了这里，就说明获取锁是成功的，我们应该再次检测redis
            // 缓存是否存在，做DoubleCheck，如果存在，则无需重新创建
            json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
            if (StrUtil.isNotBlank(json)) {
                r = JSONUtil.toBean(json, type);
                return r;
            }
            // 4.4 成功，根据id查询数据库
            r = dbFallback.apply(id);
            // 5.不存在，返回错误
            if (r == null) {
                // 将控制写入redis
                stringRedisTemplate.opsForValue().set(keyPrefix + id, "",
                        time, unit);
                return null;
            }
            // 6.存在，写入redis
            stringRedisTemplate.opsForValue().set(keyPrefix + id,
                    JSONUtil.toJsonStr(r), time, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // 7.释放互斥锁
            unLock(lockKey);
        }
        // 8.返回
        return r;
    }
}

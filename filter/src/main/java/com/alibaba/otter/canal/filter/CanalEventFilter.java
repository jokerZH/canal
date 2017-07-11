package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;

/* 数据过滤机制 */
public interface CanalEventFilter<T> {
    boolean filter(T event) throws CanalFilterException;
}

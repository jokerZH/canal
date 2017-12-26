package com.alibaba.otter.canal.common;

/* */
public interface CanalLifeCycle {
    void start();

    void stop();

    boolean isStart();
}

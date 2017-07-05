package com.alibaba.otter.canal.parse.ha;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalHAException;

/* HA 控制器实现 */
public interface CanalHAController extends CanalLifeCycle {

    public void start() throws CanalHAException;

    public void stop() throws CanalHAException;
}

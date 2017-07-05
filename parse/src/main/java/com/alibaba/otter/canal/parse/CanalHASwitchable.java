package com.alibaba.otter.canal.parse;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;

/* 支持可切换的数据复制控制器 */
public interface CanalHASwitchable {

    public void doSwitch();

    public void doSwitch(AuthenticationInfo newAuthenticationInfo);
}

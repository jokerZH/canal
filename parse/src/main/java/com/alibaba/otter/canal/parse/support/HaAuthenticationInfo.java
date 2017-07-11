package com.alibaba.otter.canal.parse.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/* */
public class HaAuthenticationInfo {
    private AuthenticationInfo       master;
    private List<AuthenticationInfo> slavers = new ArrayList<AuthenticationInfo>();

    public AuthenticationInfo getMaster() { return master; }
    public void setMaster(AuthenticationInfo master) { this.master = master; }
    public List<AuthenticationInfo> getSlavers() { return slavers; }
    public void addSlaver(AuthenticationInfo slaver) { this.slavers.add(slaver); }
    public void addSlavers(Collection<AuthenticationInfo> slavers) { this.slavers.addAll(slavers); }
}

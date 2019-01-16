package com.baosight.iwater.gxsy.jrp.pojo;

public class Pjrp {
    /**
     * 开始时间
     */
    public String startDate;
    /**
     * 结束时间
     */
    public String endDate;
    /**
     * 检查算法
     */
    public String artis;
    /**
     * 处理算法
     */
    public String deal;

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getArtis() {
        return artis;
    }

    public void setArtis(String artis) {
        this.artis = artis;
    }

    public String getDeal() {
        return deal;
    }

    public void setDeal(String deal) {
        this.deal = deal;
    }
}

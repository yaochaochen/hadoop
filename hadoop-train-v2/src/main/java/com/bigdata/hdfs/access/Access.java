package com.bigdata.hdfs.access;


public class Access {

    private String phone;

    private long up;
    private long down;

    public Access(){}
    public Access(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }
}

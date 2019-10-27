package com.entity;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 23:32
 * @Author: code1990
 * @Description:
 */
public class ResultMessage {
    private String status;//状态 fail 、 success
    private String message;//消息内容

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
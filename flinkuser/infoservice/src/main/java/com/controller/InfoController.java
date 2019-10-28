package com.controller;

import com.alibaba.fastjson.JSONObject;
import com.entity.ResultMessage;
import com.util.ReadProperties;
import log.AttentionProductLog;
import log.BuyCartProductLog;
import log.CollectProductLog;
import log.ScanProductLog;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 17:50
 * @Author: code1990
 * @Description:
 */
@RestController
@RequestMapping("/info")
public class InfoController {
    private final String attentionProductLogTopic = ReadProperties.getKey("attentionProductLog");
    private final String buyCartProductLogTopic = ReadProperties.getKey("buyCartProductLog");
    private final String collectProductLogTopic = ReadProperties.getKey("collectProductLog");
    private final String scanProductLogTopic = ReadProperties.getKey("scanProductLog");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "hello", method = RequestMethod.GET)
    public String hello(HttpServletRequest request) {
        String ip = request.getRemoteAddr();
        return "hello:" + ip + "success";
    }

    @RequestMapping(value = "receivelog", method = RequestMethod.POST)
    public String hellowolrd(String recevicelog, HttpServletRequest req) {
        if (StringUtils.isBlank(recevicelog)) {
            return null;
        }
        String[] rearrays = recevicelog.split(":", 2);
        String classname = rearrays[0];
        String data = rearrays[1];
        String resulmesage = "";

        if ("AttentionProductLog".equals(classname)) {
            AttentionProductLog attentionProductLog = JSONObject.parseObject(data, AttentionProductLog.class);
            resulmesage = JSONObject.toJSONString(attentionProductLog);
            kafkaTemplate.send(attentionProductLogTopic, resulmesage + "##1##" + System.currentTimeMillis());
        } else if ("BuyCartProductLog".equals(classname)) {
            BuyCartProductLog buyCartProductLog = JSONObject.parseObject(data, BuyCartProductLog.class);
            resulmesage = JSONObject.toJSONString(buyCartProductLog);
            kafkaTemplate.send(buyCartProductLogTopic, resulmesage + "##1##" + System.currentTimeMillis());
        } else if ("CollectProductLog".equals(classname)) {
            CollectProductLog collectProductLog = JSONObject.parseObject(data, CollectProductLog.class);
            resulmesage = JSONObject.toJSONString(collectProductLog);
            kafkaTemplate.send(collectProductLogTopic, resulmesage + "##1##" + System.currentTimeMillis());
        } else if ("ScanProductLog".equals(classname)) {
            ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
            resulmesage = JSONObject.toJSONString(scanProductLog);
            kafkaTemplate.send(scanProductLogTopic, resulmesage + "##1##" + System.currentTimeMillis());
        }
        ResultMessage resultMessage = new ResultMessage();
        resultMessage.setMessage(resulmesage);
        resultMessage.setStatus("success");
        String result = JSONObject.toJSONString(resultMessage);
        return result;
    }
}

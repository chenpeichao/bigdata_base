package org.pcchen.controller;

import org.pcchen.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ceek
 * @date 2021/3/6 17:29
 */
@RestController
@RequestMapping("/publisher")
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @GetMapping("/getDauTotal")
    @ResponseBody
    public Map<String, Object> getDauTotal(@RequestParam("searchDate") String searchDate) {
        Map<String, Object> resultMap = new HashMap<String, Object>();

        Integer dauTotal = publisherService.getDauTotal(searchDate, "gmall_test");
        resultMap.put("dauTotal", dauTotal);
        return resultMap;
    }

    @GetMapping("/getDauHour")
    @ResponseBody
    public Map<String, Object> getDauHour(@RequestParam("searchDate") String searchDate) {
        Map<String, Object> resultMap = new HashMap<String, Object>();

        Map<String, Object> dauHourResult = publisherService.getDauHour(searchDate, "gmall_test");
        resultMap.put("dauHour", dauHourResult);
        return resultMap;
    }
}
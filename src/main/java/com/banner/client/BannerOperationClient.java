package com.banner.client;

import com.banner.common.dto.request.BannerSyncRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Component
public class BannerOperationClient {

    private static final Logger log = LoggerFactory.getLogger(BannerOperationClient.class);

    private static final String BASE_URL = "http://operation-service/api/banner";

    private final RestTemplate restTemplate = new RestTemplate();

    public BannerSyncRequest getBannerById(Long id) {
        if (Objects.isNull(id)) {
            log.warn("getBannerById failed, id is null");
            return null;
        }
        try {
            String url = String.format("%s/%d", BASE_URL, id);
            return restTemplate.getForObject(url, BannerSyncRequest.class);
        } catch (Exception e) {
            log.error("getBannerById failed, id={}", id, e);
            return null;
        }
    }

    /**
     * 分页获取banner的用户列表
     */
    public List<Long> getUserIdsByPage(Long id, int page, int size) {
        if (Objects.isNull(id)) {
            log.warn("getUserIdsByPage failed, id is null");
            return Collections.emptyList();
        }
        try {
            String url = String.format("%s/%d/users?page=%d&size=%d", BASE_URL, id, page, size);
            ResponseEntity<List<Long>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<>() {}
            );
            return Objects.isNull(response.getBody()) ? Collections.emptyList() : response.getBody();
        } catch (Exception e) {
            log.error("getUserIdsByPage failed, id={}, page={}, size={}", id, page, size, e);
            return Collections.emptyList();
        }
    }

    public List<Long> getIncrementalUpdatedIds(long sinceTime) {
        try {
            String url = String.format("%s/incremental?since=%d", BASE_URL, sinceTime);
            ResponseEntity<List<Long>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<>() {}
            );
            return Objects.isNull(response.getBody()) ? Collections.emptyList() : response.getBody();
        } catch (Exception e) {
            log.error("getIncrementalUpdatedIds failed, sinceTime={}", sinceTime, e);
            return Collections.emptyList();
        }
    }

    public List<Long> getAllActiveBannerIds() {
        try {
            String url = String.format("%s/active-ids", BASE_URL);
            ResponseEntity<List<Long>> response = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<>() {}
            );
            return Objects.isNull(response.getBody()) ? Collections.emptyList() : response.getBody();
        } catch (Exception e) {
            log.error("getAllActiveBannerIds failed", e);
            return Collections.emptyList();
        }
    }
}

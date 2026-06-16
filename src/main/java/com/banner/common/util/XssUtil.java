package com.banner.common.util;

import org.apache.commons.text.StringEscapeUtils;

/**
 * XSS防护工具类
 */
public class XssUtil {

    private static final String[] ALLOWED_PROTOCOLS = {"http", "https"};

    public static String escapeHtml(String input) {
        if (input == null) {
            return null;
        }
        return StringEscapeUtils.escapeHtml4(input);
    }

    public static String sanitizeUrl(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }
        try {
            java.net.URL parsedUrl = new java.net.URL(url);
            String protocol = parsedUrl.getProtocol().toLowerCase();
            for (String allowed : ALLOWED_PROTOCOLS) {
                if (allowed.equals(protocol)) {
                    return url;
                }
            }
            return null;
        } catch (java.net.MalformedURLException e) {
            return null;
        }
    }

    public static String sanitizeLinkUrl(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }
        if (url.toLowerCase().startsWith("javascript:")) {
            return null;
        }
        return sanitizeUrl(url);
    }

    public static String stripHtml(String input) {
        if (input == null) {
            return null;
        }
        return input.replaceAll("<[^>]*>", "");
    }
}

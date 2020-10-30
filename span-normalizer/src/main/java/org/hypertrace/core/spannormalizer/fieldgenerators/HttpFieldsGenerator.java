package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Envoy.ENVOY_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_REQUEST_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_HTTP_RESPONSE_BODY;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_AUTHORITY_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HOST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PARAM;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_PATH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_QUERY_STRING;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_X_FORWARDED_FOR_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_CONTENT_TYPE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_HEADER_SET_COOKIE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_SIZE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_DASH;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_AGENT_WITH_UNDERSCORE;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_USER_DOT_AGENT;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_METHOD;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpFieldsGenerator extends ProtocolFieldsGenerator<Http.Builder> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpFieldsGenerator.class);

  private static final char DOT = '.';
  private static final String REQUEST_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + DOT;
  private static final String RESPONSE_HEADER_PREFIX =
      RawSpanConstants.getValue(HTTP_RESPONSE_HEADER) + DOT;
  private static final String REQUEST_PARAM_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_PARAM) + DOT;
  private static final String REQUEST_COOKIE_PREFIX =
      RawSpanConstants.getValue(HTTP_REQUEST_COOKIE) + DOT;
  private static final String RESPONSE_COOKIE_PREFIX =
      RawSpanConstants.getValue(HTTP_RESPONSE_COOKIE) + DOT;
  private static final String SLASH = "/";
  private static URL dummyUrl;

  static {
    try {
      dummyUrl = new URL("http://example.com");
    } catch (MalformedURLException e) {
      // ignore
    }
  }

  private static final List<String> FULL_URL_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
          RawSpanConstants.getValue(HTTP_REQUEST_URL),
          RawSpanConstants.getValue(HTTP_URL));

  private static final List<String> USER_AGENT_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
          RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
          RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER),
          RawSpanConstants.getValue(HTTP_USER_AGENT));

  private static final List<String> URL_PATH_ATTRIBUTES =
      List.of(RawSpanConstants.getValue(HTTP_REQUEST_PATH), RawSpanConstants.getValue(HTTP_PATH));

  private static final List<String> REQUEST_SIZE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
          RawSpanConstants.getValue(HTTP_REQUEST_SIZE));

  private static final List<String> RESPONSE_SIZE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
          RawSpanConstants.getValue(HTTP_RESPONSE_SIZE));

  private static final List<String> STATUS_CODE_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE),
          RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE));

  private static final List<String> METHOD_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(HTTP_REQUEST_METHOD),
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD));

  private static Map<String, FieldGenerator<Http.Builder>> fieldGeneratorMap =
      initializeFieldGenerators();

  public boolean handleStartsWithKeyIfNecessary(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Event.Builder eventBuilder) {
    if (key.startsWith(REQUEST_HEADER_PREFIX)) {
      handleRequestHeader(key, keyValue, getProtocolBuilder(eventBuilder));
      return true;
    } else if (key.startsWith(RESPONSE_HEADER_PREFIX)) {
      handleResponseHeader(key, keyValue, getProtocolBuilder(eventBuilder));
      return true;
    } else if (key.startsWith(REQUEST_PARAM_PREFIX)) {
      handleRequestParam(key, keyValue, getProtocolBuilder(eventBuilder));
      return true;
    } else if (key.startsWith(REQUEST_COOKIE_PREFIX)) {
      handleRequestCookie(key, keyValue, getProtocolBuilder(eventBuilder));
      return true;
    } else if (key.startsWith(RESPONSE_COOKIE_PREFIX)) {
      handleResponseCookie(key, keyValue, getProtocolBuilder(eventBuilder));
      return true;
    }

    return false;
  }

  void handleRequestHeader(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Http.Builder httpBuilder) {
    getSuffix(key, REQUEST_HEADER_PREFIX)
        .ifPresent(
            header ->
                httpBuilder
                    .getRequestBuilder()
                    .getHeadersBuilder()
                    .getOtherHeaders()
                    .put(header, keyValue.getVStr()));
  }

  void handleResponseHeader(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Http.Builder httpBuilder) {
    getSuffix(key, RESPONSE_HEADER_PREFIX)
        .ifPresent(
            header ->
                httpBuilder
                    .getResponseBuilder()
                    .getHeadersBuilder()
                    .getOtherHeaders()
                    .put(header, keyValue.getVStr()));
  }

  void handleRequestParam(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Http.Builder httpBuilder) {
    getSuffix(key, REQUEST_PARAM_PREFIX)
        .ifPresent(
            param -> httpBuilder.getRequestBuilder().getParams().put(param, keyValue.getVStr()));
  }

  void handleRequestCookie(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Http.Builder httpBuilder) {
    getSuffix(key, REQUEST_COOKIE_PREFIX)
        .ifPresent(
            cookieKey ->
                httpBuilder
                    .getRequestBuilder()
                    .getCookies()
                    .add(cookieKey + "=" + ValueConverter.getString(keyValue)));
  }

  void handleResponseCookie(
      String key, JaegerSpanInternalModel.KeyValue keyValue, Http.Builder httpBuilder) {
    getSuffix(key, RESPONSE_COOKIE_PREFIX)
        .ifPresent(
            cookieKey ->
                httpBuilder
                    .getResponseBuilder()
                    .getCookies()
                    .add(cookieKey + "=" + ValueConverter.getString(keyValue)));
  }

  private Optional<String> getSuffix(String key, String prefix) {
    if (key.startsWith(prefix) && key.length() > prefix.length()) {
      return Optional.of(key.substring(prefix.length()));
    }

    return Optional.empty();
  }

  @Override
  protected Http.Builder getProtocolBuilder(Event.Builder eventBuilder) {
    Http.Builder httpBuilder = eventBuilder.getHttpBuilder();

    if (!httpBuilder.getRequestBuilder().getHeadersBuilder().hasOtherHeaders()
        || !httpBuilder.getRequestBuilder().hasParams()
        || !httpBuilder.getRequestBuilder().hasCookies()
        || !httpBuilder.getResponseBuilder().getHeadersBuilder().hasOtherHeaders()
        || !httpBuilder.getResponseBuilder().hasCookies()) {
      httpBuilder.getRequestBuilder().getHeadersBuilder().setOtherHeaders(new HashMap<>());
      httpBuilder.getRequestBuilder().setParams(new HashMap<>());
      httpBuilder.getRequestBuilder().setCookies(new ArrayList<>());

      httpBuilder.getResponseBuilder().getHeadersBuilder().setOtherHeaders(new HashMap<>());
      httpBuilder.getResponseBuilder().setCookies(new ArrayList<>());
    }

    return httpBuilder;
  }

  @Override
  protected Map<String, FieldGenerator<Http.Builder>> getFieldGeneratorMap() {
    return fieldGeneratorMap;
  }

  void populateOtherFields(Event.Builder eventBuilder) {
    populateUrlParts(eventBuilder.getHttpBuilder().getRequestBuilder());
  }

  private static Map<String, FieldGenerator<Http.Builder>> initializeFieldGenerators() {
    Map<String, FieldGenerator<Http.Builder>> fieldGeneratorMap = new HashMap<>();

    // Method Handlers
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_METHOD),
        (key, keyValue, builder, tagsMap) -> setMethod(builder, tagsMap));
    // OT_SPAN_TAG_HTTP_METHOD == HTTP_METHOD. No need to have both
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_METHOD),
        (key, keyValue, builder, tagsMap) -> setMethod(builder, tagsMap));

    // URL Handlers
    // OT_SPAN_TAG_HTTP_URL == HTTP_URL_WITH_HTTP. No need to have both
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
        (key, keyValue, builder, tagsMap) -> setUrl(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_URL),
        (key, keyValue, builder, tagsMap) -> setUrl(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_URL),
        (key, keyValue, builder, tagsMap) -> setUrl(builder, tagsMap));

    // URL Path
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_PATH),
        (key, keyValue, builder, tagsMap) -> setPath(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_PATH),
        (key, keyValue, builder, tagsMap) -> setPath(builder, tagsMap));

    // User Agent handlers
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_USER_DOT_AGENT),
        (key, keyValue, builder, tagsMap) -> setUserAgent(builder, tagsMap));
    // HTTP_USER_AGENT_WITH_UNDERSCORE == ENVOY_USER_AGENT. No need to have both
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_UNDERSCORE),
        (key, keyValue, builder, tagsMap) -> setUserAgent(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_WITH_DASH),
        (key, keyValue, builder, tagsMap) -> setUserAgent(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT_REQUEST_HEADER),
        (key, keyValue, builder, tagsMap) -> {
          setUserAgent(builder, tagsMap);
          builder.getRequestBuilder().getHeadersBuilder().setUserAgent(keyValue.getVStr());
        });
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_USER_AGENT),
        (key, keyValue, builder, tagsMap) -> setUserAgent(builder, tagsMap));

    // Declared Request Headers
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HOST_HEADER),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setHost(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_AUTHORITY_HEADER),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setAuthority(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_CONTENT_TYPE),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setContentType(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER_PATH),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setPath(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_X_FORWARDED_FOR_HEADER),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setXForwardedFor(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER_COOKIE),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().getHeadersBuilder().setCookie(keyValue.getVStr()));

    // Declared Response Headers
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_CONTENT_TYPE),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().getHeadersBuilder().setContentType(keyValue.getVStr()));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_HEADER_SET_COOKIE),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().getHeadersBuilder().setSetCookie(keyValue.getVStr()));

    // Request Body
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_HTTP_REQUEST_BODY),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().setBody(keyValue.getVStr()));

    // Response Body
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_HTTP_RESPONSE_BODY),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setBody(keyValue.getVStr()));

    // Request Size
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(ENVOY_REQUEST_SIZE),
        (key, keyValue, builder, tagsMap) -> setRequestSize(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_SIZE),
        (key, keyValue, builder, tagsMap) -> setRequestSize(builder, tagsMap));

    // Response Size
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(ENVOY_RESPONSE_SIZE),
        (key, keyValue, builder, tagsMap) -> setResponseSize(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_SIZE),
        (key, keyValue, builder, tagsMap) -> setResponseSize(builder, tagsMap));

    // Response status and status code
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_STATUS_CODE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_CODE),
        (key, keyValue, builder, tagsMap) -> setResponseStatusCode(builder, tagsMap));
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_RESPONSE_STATUS_MESSAGE),
        (key, keyValue, builder, tagsMap) ->
            builder.getResponseBuilder().setStatusMessage(ValueConverter.getString(keyValue)));

    // Other declared Request fields
    fieldGeneratorMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_QUERY_STRING),
        (key, keyValue, builder, tagsMap) ->
            builder.getRequestBuilder().setQueryString(ValueConverter.getString(keyValue)));

    // Other declared Response fields

    return fieldGeneratorMap;
  }

  private static void setUrl(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getRequestBuilder().hasUrl()) {
      return;
    }

    FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            tagsMap, FULL_URL_ATTRIBUTES,
            // even though relative URLs are allowed here, they are eventually unset in populateUrlParts method
            s -> !StringUtils.isBlank(s) && isValidUrl(s))
        .ifPresent(url -> httpBuilder.getRequestBuilder().setUrl(url));
  }

  private static void setMethod(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getRequestBuilder().hasMethod()) {
      return;
    }

    FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            tagsMap, METHOD_ATTRIBUTES, s -> !StringUtils.isBlank(s))
        .ifPresent(method -> httpBuilder.getRequestBuilder().setMethod(method));
  }

  private static void setUserAgent(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getRequestBuilder().hasUserAgent()) {
      return;
    }

    FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(tagsMap, USER_AGENT_ATTRIBUTES)
        .ifPresent(userAgent -> httpBuilder.getRequestBuilder().setUserAgent(userAgent));
  }

  private static void setPath(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getRequestBuilder().hasPath()) {
      return;
    }

    Optional<String> pathFromAttrs =
        FirstMatchingKeyFinder.getStringValueByFirstMatchingKey(
            tagsMap, URL_PATH_ATTRIBUTES, s -> StringUtils.isNotBlank(s) && s.startsWith(SLASH));
    if (pathFromAttrs.isEmpty()) {
      return;
    }

    getPathFromUrlObject(pathFromAttrs.get())
        .map(HttpFieldsGenerator::removeTrailingSlash)
        .ifPresent(path -> httpBuilder.getRequestBuilder().setPath(path));
  }

  private static String removeTrailingSlash(String s) {
    // Ends with "/" and it's not home page path
    return s.endsWith(SLASH) && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
  }

  private static Optional<String> getPathFromUrlObject(String urlPath) {
    try {
      URL url = new URL(dummyUrl, urlPath);
      return Optional.of(url.getPath());
    } catch (MalformedURLException e) {
      return Optional.empty();
    }
  }

  private static void setRequestSize(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getRequestBuilder().hasSize()) {
      return;
    }

    FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(tagsMap, REQUEST_SIZE_ATTRIBUTES)
        .ifPresent(size -> httpBuilder.getRequestBuilder().setSize(size));
  }

  private static void setResponseSize(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getResponseBuilder().hasSize()) {
      return;
    }

    FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(tagsMap, RESPONSE_SIZE_ATTRIBUTES)
        .ifPresent(size -> httpBuilder.getResponseBuilder().setSize(size));
  }

  private static void setResponseStatusCode(
      Http.Builder httpBuilder, Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    if (httpBuilder.getResponseBuilder().hasStatusCode()) {
      return;
    }

    FirstMatchingKeyFinder.getIntegerValueByFirstMatchingKey(tagsMap, STATUS_CODE_ATTRIBUTES)
        .ifPresent(statusCode -> httpBuilder.getResponseBuilder().setStatusCode(statusCode));
  }

  /**
   * accepts any absolute or relative URL
   */
  private static boolean isValidUrl(String url) {
    try {
      new URL(dummyUrl, url);
    } catch (MalformedURLException e) {
      return false;
    }
    return true;
  }

  private void setPathFromUrl(Request.Builder requestBuilder, URL url) {
    if (requestBuilder.hasPath()) { // If path was previously set, no need to set it again.
      return;
    }

    String path = url.getPath();
    if (StringUtils.isBlank(path)) {
      path = SLASH;
    }

    requestBuilder.setPath(removeTrailingSlash(path));
  }

  private void populateUrlParts(Request.Builder requestBuilder) {
    if (!requestBuilder.hasUrl()) {
      return;
    }

    String urlStr = requestBuilder.getUrl();
    try {
      URL url;
      if (isAbsoluteUrl(urlStr)) {
        url = new URL(urlStr);
        requestBuilder.setScheme(url.getProtocol());
        requestBuilder.setHost(url.getAuthority()); // Use authority so in case the port is specified it adds it to this
      } else {    // relative URL
        url = new URL(dummyUrl, urlStr);
        requestBuilder.setUrl(null); //  unset the URL as we only allow absolute/full URLs in the url field
      }
      setPathFromUrl(requestBuilder, url);
      if (!requestBuilder.hasQueryString()) {
        requestBuilder.setQueryString(url.getQuery());
      }
    } catch (MalformedURLException e) {
      // Should not happen Since the url in the request should be valid.
      LOGGER.error("Error populating url parts", e);
    }
  }

  private boolean isAbsoluteUrl(String url) {
    try {
      new URL(url);
      return true;
    } catch (MalformedURLException e) {
      return false;
    }
  }
}

package com.kadirov;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kadirov.ErrorMessages.HTTP_CLIENT_CALL_ERROR_MESSAGE;
import static com.kadirov.ErrorMessages.JSON_FORMAT_ERROR_MESSAGE;

public interface CrptApi {
    boolean createDocument(Document document, String sign);
}

class CrptApiImpl implements CrptApi, Runnable {

    public static final String CRPT_API_URL = "https://ismp.crpt.ru/api/v3/";
    public static final String DOCUMENTS_API_URL = CRPT_API_URL + "lk/documents/";
    public static final String POST_METHOD = "POST";
    public static final String DOCUMENT_ACTION = "create";
    public static final String JSON_MEDIA_TYPE = "application/json";

    private static final Logger logger = LoggerFactory.getLogger(CrptApiImpl.class);

    private final AtomicInteger calls;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int requestLimit;

    public CrptApiImpl(TimeUnit timeUnit, int requestLimit) {
        this.requestLimit = Math.max(requestLimit, 0);
        this.httpClient = new OkHttpClient.Builder().build();
        this.objectMapper = new ObjectMapper();
        this.calls = new AtomicInteger(0);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this, 1, 1, timeUnit);
    }

    @Override
    public boolean createDocument(Document document, String sign) {
        if (calls.incrementAndGet() > requestLimit) {
            logger.info("Failed to make '/documents/create' call, cause request limit '{}'", requestLimit);
            return false;
        }

        MediaType mediaType = MediaType.parse(JSON_MEDIA_TYPE);
        RequestBody requestBody = RequestBody.create(convertToJsonString(document), mediaType);

        Request request = new Request.Builder().url(DOCUMENTS_API_URL + DOCUMENT_ACTION).method(POST_METHOD, requestBody).build();

        try (Response response = makeCall(request)) {
            logger.info("Received the response status code '{}' from '/documents/create' call", response.code());
        }

        return true;
    }

    @Override
    public void run() {
        calls.set(0);
    }

    @NotNull
    private Response makeCall(@NotNull Request request) {
        Response response;

        try {
            response = httpClient.newCall(request).execute();
        } catch (IOException ioe) {
            throw new HttpClientCallException(HTTP_CLIENT_CALL_ERROR_MESSAGE, ioe);
        }

        return response;
    }

    @NotNull
    private <T> String convertToJsonString(@NotNull T object) throws JsonFormatException {
        StringWriter jsonBodyWriter = new StringWriter();

        try {
            objectMapper.writeValue(jsonBodyWriter, object);
        } catch (IOException ioe) {
            String formattedMessage = String.format(JSON_FORMAT_ERROR_MESSAGE, object.getClass().getName());
            throw new JsonFormatException(formattedMessage, ioe);
        }

        return jsonBodyWriter.toString();
    }
}

@JsonAutoDetect
record Description(
        @JsonProperty("participantInn")
        String participantInn
) { }
@JsonAutoDetect
record Document(
        @JsonProperty("description")
        Description description,
        @JsonProperty("doc_id")
        String docId,
        @JsonProperty("doc_status")
        String docStatus,
        @JsonProperty("doc_type")
        DocumentType docType,
        @JsonProperty("importRequest")
        boolean importRequest,
        @JsonProperty("owner_inn")
        String ownerInn,
        @JsonProperty("participant_inn")
        String participantInn,
        @JsonProperty("producer_inn")
        String producerInn,
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        Date productionDate,
        @JsonProperty("production_type")
        ProductionType productionType,
        @JsonProperty("products")
        List<Product> products,
        @JsonProperty("reg_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        Date regDate,
        @JsonProperty("reg_number")
        String regNumber) { }
@JsonAutoDetect
enum DocumentType {
    LP_INTRODUCE_GOODS
}
@JsonAutoDetect
record Product(
        @JsonProperty("certificate_document")
        String certificateDocument,
        @JsonProperty("certificate_document_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        Date certificateDocumentDate,
        @JsonProperty("certificate_document_number")
        String certificateDocumentNumber,
        @JsonProperty("owner_inn")
        String ownerInn,
        @JsonProperty("producer_inn")
        String producerInn,
        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        Date productionDate,
        @JsonProperty("tnved_code")
        String tnvedCode,
        @JsonProperty("uit_code")
        String uitCode,
        @JsonProperty("uitu_code")
        String uituCode) { }
@JsonAutoDetect
enum ProductionType {
    STRING
}

class ErrorMessages {
    public static final String JSON_FORMAT_ERROR_MESSAGE = "Failed to format object with class %s to json string";
    public static final String HTTP_CLIENT_CALL_ERROR_MESSAGE = "Failed to make a call";

}
class HttpClientCallException extends RuntimeException {

    public HttpClientCallException() {
    }

    public HttpClientCallException(String message) {
        super(message);
    }
    public HttpClientCallException(String message, Throwable cause) {
        super(message, cause);
    }

}
class JsonFormatException extends RuntimeException {

    public JsonFormatException() {
    }

    public JsonFormatException(String message) {
        super(message);
    }
    public JsonFormatException(String message, Throwable cause) {
        super(message, cause);
    }

}


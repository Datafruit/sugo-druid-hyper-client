package io.druid.hyper.client.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

public class JsonObjectIterator implements Iterator, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JsonObjectIterator.class);
    private static final TypeReference<HashMap> typeRef = new TypeReference<HashMap>() { };

    private final InputStream inputStream;
    private JsonParser jsonParser;
    private volatile boolean isInitialized;

    private HashMap nextObject;

    public JsonObjectIterator(final InputStream inputStream) {
        this.inputStream = inputStream;
        this.isInitialized = false;
        this.nextObject = null;
    }

    private void init() {
        this.initJsonParser();
        this.initFirstElement();
        this.isInitialized = true;
    }

    private void initJsonParser() {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonFactory jsonFactory = objectMapper.getFactory();

        try {
            this.jsonParser = jsonFactory.createParser(inputStream);
        } catch (final IOException e) {
            LOG.error("There was a problem setting up the JsonParser: " + e.getMessage(), e);
            throw new RuntimeException("There was a problem setting up the JsonParser: " + e.getMessage(), e);
        }
    }

    private void initFirstElement() {
        try {
            // Check that the first element is the start of an array
            final JsonToken arrayStartToken = this.jsonParser.nextToken();
            if (arrayStartToken != JsonToken.START_ARRAY) {
                throw new IllegalStateException("The first element of the Json structure was expected to be a start array token, but it was: " + arrayStartToken);
            }

            // Initialize the first object
            this.initNextObject();
        } catch (final Exception e) {
            LOG.error("There was a problem initializing the first element of the Json Structure: " + e.getMessage(), e);
            throw new RuntimeException("There was a problem initializing the first element of the Json Structure: " + e.getMessage(), e);
        }
    }

    private void initNextObject() {
        try {
            final JsonToken nextToken = this.jsonParser.nextToken();

            // Check for the end of the array which will mean we're done
            if (nextToken == JsonToken.END_ARRAY) {
                this.nextObject = null;
                return;
            }

            // Make sure the next token is the start of an object
            if (nextToken != JsonToken.START_OBJECT) {
                throw new IllegalStateException("The next token of Json structure was expected to be a start object token, but it was: " + nextToken);
            }

            // Get the next product and make sure it's not null
            this.nextObject = this.jsonParser.readValueAs(typeRef);
            if (this.nextObject == null) {
                throw new IllegalStateException("The next parsed object of the Json structure was null");
            }
        } catch (final Exception e) {
            LOG.error("There was a problem initializing the next Object: " + e.getMessage(), e);
            throw new RuntimeException("There was a problem initializing the next Object: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        if (!this.isInitialized) {
            this.init();
        }

        return this.nextObject != null;
    }

    @Override
    public HashMap next() {
        // This method will return the current object and initialize the next object so hasNext will always have knowledge of the current state

        // Makes sure we're initialized first
        if (!this.isInitialized) {
            this.init();
        }

        // Store the current next object for return
        final HashMap currentNextObject = this.nextObject;

        // Initialize the next object
        this.initNextObject();

        return currentNextObject;
    }

    @Override
    public void close() throws IOException {
        closeQuietly(this.jsonParser);
        closeQuietly(this.inputStream);
    }

    @Override
    public void remove() {

    }

    private void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignore) {
        }
    }

}

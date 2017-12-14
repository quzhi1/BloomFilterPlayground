package com.quzhi.bloomfilterplaygound;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.io.IOUtils;
import org.apache.commons.jxpath.JXPathContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class Main {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final double FP_PROBABILITY = 0.01;

    private Main() {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) throws IOException {
        Main main = new Main();
        main.bloom();

    }

    private void bloom() throws IOException{
        List<String> pageOneUuids = readList("pageOne.json");
        List<String> pageTwoUuids = readList("pageTwo.json");
        BloomFilter<String> uuidFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8),
                                                            pageOneUuids.size(),
                                                            FP_PROBABILITY);
        pageOneUuids.forEach(uuidFilter::put);
        System.out.println("bloom filter: " + serializeFilter(uuidFilter));
        pageTwoUuids.forEach(pageTwoUuid ->
            System.out.println("Contains " + pageTwoUuid +  "? " + uuidFilter.mightContain(pageTwoUuid))
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> readList(final String location) throws IOException {
        InputStream in = getClass().getClassLoader().getResourceAsStream(location);
        String jsonStr = IOUtils.toString(in);
        JXPathContext context = JXPathContext.newContext(MAPPER.readValue(jsonStr, Map.class));
        return (List<String>) context.getValue("/uuids");
    }

    private String serializeFilter(final BloomFilter<String> uuidFilter) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        uuidFilter.writeTo(byteStream);
        byteStream.close();
        return Base64.getUrlEncoder().encodeToString(byteStream.toByteArray());
    }
}

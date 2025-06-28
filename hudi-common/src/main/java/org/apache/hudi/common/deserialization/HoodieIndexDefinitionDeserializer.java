package org.apache.hudi.common.deserialization;

import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.metadata.HoodieIndexVersion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HoodieIndexDefinitionDeserializer extends JsonDeserializer<HoodieIndexDefinition> {

  @Override
  public HoodieIndexDefinition deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    ObjectMapper mapper = (ObjectMapper) jp.getCodec();
    JsonNode node = mapper.readTree(jp);
    
    // Parse fields manually to avoid recursion
    String indexName = node.get("indexName").asText();
    String indexType = node.get("indexType").asText();
    String indexFunction = node.has("indexFunction") ? node.get("indexFunction").asText() : "";
    
    List<String> sourceFields = new ArrayList<>();
    if (node.has("sourceFields") && node.get("sourceFields").isArray()) {
      for (JsonNode field : node.get("sourceFields")) {
        sourceFields.add(field.asText());
      }
    }
    
    Map<String, String> indexOptions = new HashMap<>();
    if (node.has("indexOptions") && node.get("indexOptions").isObject()) {
      JsonNode optionsNode = node.get("indexOptions");
      optionsNode.fieldNames().forEachRemaining(key -> 
          indexOptions.put(key, optionsNode.get(key).asText()));
    }
    
    // Handle version field - if missing or null, use V1 (table version 8 default)
    HoodieIndexVersion version = HoodieIndexVersion.V1; // default
    if (node.has("version") && !node.get("version").isNull()) {
      String versionStr = node.get("version").asText();
      version = HoodieIndexVersion.valueOf(versionStr);
    }
    
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType(indexType)
        .withIndexFunction(indexFunction)
        .withSourceFields(sourceFields)
        .withIndexOptions(indexOptions)
        .withVersion(version)
        .build();
  }
}

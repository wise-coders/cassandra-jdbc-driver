package com.wisecoders.dbschema.cassandra;

import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class AWSUtil {

    /**
     * Get the Secret Value
     * @param regionName
     * @param secretName
     * @param secretKey
     * @return
     */
    public static String getSecretValue(String regionName, String secretName, String secretKey) {
        Region region = Region.of(regionName);
        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();
        return getValue(secretsClient, secretName, secretKey);
    }

    public static String getValue(SecretsManagerClient secretsClient, String secretName, String secretKey) {
        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();
            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> map = mapper.readValue(secret, Map.class);
            secretsClient.close();
            return map.get(secretKey);
        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        } catch (JsonProcessingException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

}

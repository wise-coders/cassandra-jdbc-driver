package com.wisecoders.dbschema.cassandra;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

public class AWSUtil {

    public static String getSecretValue(String regionName, String secretName) {
        Region region = Region.of(regionName);
        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();
        return getValue(secretsClient, secretName);
    }

    public static String getValue(SecretsManagerClient secretsClient, String secretName) {
        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();
            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            secretsClient.close();
            return secret;
        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

}

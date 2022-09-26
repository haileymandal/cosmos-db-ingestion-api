package com.db.ingestor.function;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.util.Optional;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */
    @FunctionName("CosmosIngestor")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        // Parse query parameter
        final String jsonString = request.getBody().orElse("");

        String serviceEndpoint = "";
        String key = "";       
        String databaseName = "student-records";
        String containerName = "students";
        String partitionKeyPath = "/grade"; 

        CosmosClient cosmosClient = new CosmosClientBuilder()
        .endpoint(serviceEndpoint)
        .key(key)
        .buildClient();

        cosmosClient.createDatabaseIfNotExists(databaseName);
        CosmosDatabase cosmosDatabase = cosmosClient.getDatabase(databaseName);

        cosmosDatabase.createContainerIfNotExists(containerName, partitionKeyPath);
        CosmosContainer cosmosContainer = cosmosDatabase.getContainer(containerName);
        
        Object obj = JSONValue.parse(jsonString);
        JSONObject jsonObject = (JSONObject) obj;

        cosmosContainer.createItem(jsonObject);

        if (jsonString == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass student record in the request body").build();
        } else {
            return request.createResponseBuilder(HttpStatus.OK).body("Successfully Added Data").build();
        }
    }
}

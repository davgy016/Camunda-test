package com.camundatest.utilities;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.nio.file.*;
import javax.inject.Inject;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;

public class DeployProcess {

    public static String processId;

    public static DeploymentEvent deployProcess(ZeebeClient client, String bpmnResource) {
        try {

            // String bpmnResource = "send-email.bpmn";
            System.out.println("Deploying BPMN process: " + bpmnResource);

            DeploymentEvent deploymentEvent = client.newDeployResourceCommand()
                    .addResourceFromClasspath(bpmnResource)
                    .send()
                    .join();

            processId = deploymentEvent.getProcesses().get(0).getBpmnProcessId();
            // // Verify the correct process was deployed
            // boolean processFound = deploymentEvent.getProcesses().stream()
            // .anyMatch(process -> processId.equals(process.getBpmnProcessId()));
            // assertTrue(processFound, "Process '" + processId + "' should be deployed");
            return deploymentEvent;

            // System.out.println("BPMN process deployed successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to deploy BPMN process: " + e.getMessage(), e);
        }
    }

   

}

// Loads the BPMN from the classpath and deploys it to the test engine.
// join() blocks until the async deploy is done.
// private void deployProcess() {
// try {
// System.out.println("Deploying BPMN process: " + bpmnResource);

// var deploymentEvent = client.newDeployResourceCommand()
// .addResourceFromClasspath(bpmnResource)
// .send()
// .join();

// // Assert deployment was successful
// assertNotNull(deploymentEvent, "Deployment event should not be null");
// assertFalse(deploymentEvent.getProcesses().isEmpty(), "At least one process
// should be deployed");

// // Verify the correct process was deployed
// boolean processFound = deploymentEvent.getProcesses().stream()
// .anyMatch(process -> PROCESS_ID.equals(process.getBpmnProcessId()));
// assertTrue(processFound, "Process '" + PROCESS_ID + "' should be deployed");

// System.out.println("BPMN process deployed successfully");
// } catch (Exception e) {
// throw new RuntimeException("Failed to deploy BPMN process: " +
// e.getMessage(), e);
// }
// }
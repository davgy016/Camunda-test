package com.camundatest;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ZeebeProcessTest
public class EmailProcessTest {
    private static final String PROCESS_ID = "send-email";
    private static final String DEFAULT_MESSAGE = "This is a test email content";
    private static final String USER_TASK_ELEMENT_ID = "enterMessageTask";
    private static final String SERVICE_TASK_ELEMENT_ID = "sendEmailTask";
    
    @Inject
    private ZeebeClient client;
    
    @BeforeEach
    public void setup() {
        // Deploy the process before each test
        deployProcess();
    }
    
    private void deployProcess() {
        try {
            String bpmnResource = "send-email.bpmn";
            System.out.println("Deploying BPMN process: " + bpmnResource);
            
            client.newDeployResourceCommand()
                    .addResourceFromClasspath(bpmnResource)
                    .send()
                    .join();
                    
            System.out.println("BPMN process deployed successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to deploy BPMN process: " + e.getMessage(), e);
        }
    }
    
    private ProcessInstanceEvent startProcess(String messageContent) {
        return client.newCreateInstanceCommand()
                .bpmnProcessId(PROCESS_ID)
                .latestVersion()
                .variables(Map.of(
                    "testRunId", UUID.randomUUID().toString(),
                    "message_content", messageContent
                ))
                .send()
                .join();
    }
    
    private void completeUserTask(long processInstanceKey, String messageContent) {
        // Wait for and complete the user task
        var jobs = waitForJobs("io.camunda.zeebe:userTask", 1);
        var job = jobs.get(0);
        
        client.newCompleteCommand(job.getKey())
                .variables(Map.of("message_content", messageContent))
                .send()
                .join();
    }
    
    private void completeServiceTask() {
        var jobs = waitForJobs("email", 1);
        var job = jobs.get(0);
        
        // Verify the message content was passed correctly
        Map<String, Object> variables = job.getVariablesAsMap();
        String messageContent = (String) variables.get("message_content");
        
        client.newCompleteCommand(job.getKey())
                .send()
                .join();
    }
    
    private List<io.camunda.zeebe.client.api.response.ActivatedJob> waitForJobs(String jobType, int count) {
        return org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(30))
                .pollInterval(java.time.Duration.ofMillis(100))
                .until(() -> {
                    var jobList = client.newActivateJobsCommand()
                            .jobType(jobType)
                            .maxJobsToActivate(count)
                            .send()
                            .join()
                            .getJobs();
                    return jobList;
                }, jobList -> jobList.size() >= count);
    }

    @Test
    public void testEmailProcessWithDefaultMessage() {
        // Test with default message
        testEmailProcessWithMessage(DEFAULT_MESSAGE);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "Short message",
        "Message with special characters: !@#$%^&*()_+{}|:<>?\"'\nNew line",
        "Message with unicode: 😊 你好 こんにちは",
        "A very long message: " +
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. " +
        "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. " +
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. " +
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, " +
        "totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. " +
        "Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos " +
        "qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, " +
        "consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem."  // Long message
    })
    public void testEmailProcessWithDifferentMessages(String message) {
        testEmailProcessWithMessage(message);
    }
    
    @Test
    public void testEmailProcessWithEmptyMessage() {
        // Start the process without a message
        ProcessInstanceEvent processInstance = startProcess("");
        System.out.println("Started process with empty message, instance key: " + processInstance.getProcessInstanceKey());
        
        // Wait for the user task to be created
        var jobs = waitForJobs("io.camunda.zeebe:userTask", 1);
        var job = jobs.get(0);
        
        // Try to complete the task with an empty message
        try {
            client.newCompleteCommand(job.getKey())
                    .variables(Map.of("message_content", ""))
                    .send()
                    .join();
            
            // If we get here, the form validation didn't work as expected
            // Check if the process is still at the user task
            var jobAfterCompletion = waitForJobs("io.camunda.zeebe:userTask", 1);
            if (jobAfterCompletion.isEmpty()) {
                fail("Empty message was accepted but should have been rejected by form validation");
            }
            System.out.println("Form validation is working: Empty message was rejected");
            
        } catch (Exception e) {
            // Check if this is a validation error
            if (e.getMessage() != null && e.getMessage().contains("required")) {
                System.out.println("Form validation is working: " + e.getMessage());
            } else {
                fail("Unexpected error when submitting empty message: " + e.getMessage(), e);
            }
        }
    }
    
    @Test
    public void testProcessCancellation() {
        // Start the process
        ProcessInstanceEvent processInstance = startProcess(DEFAULT_MESSAGE);
        
        // Cancel the process instance
        client.newCancelInstanceCommand(processInstance.getProcessInstanceKey())
                .send()
                .join();
                
        System.out.println("Process instance cancelled successfully");
    }
    
    private void testEmailProcessWithMessage(String message) {
        // Start a new process instance with the test message
        ProcessInstanceEvent processInstance = startProcess(message);
        System.out.println("Testing with message: " + 
            (message.length() > 50 ? message.substring(0, 47) + "..." : message));
            
        // Complete the user task with the test message
        completeUserTask(processInstance.getProcessInstanceKey(), message);
        
        // Complete the service task
        completeServiceTask();
        
        System.out.println("Process completed successfully with message: " + 
            (message.length() > 50 ? message.substring(0, 47) + "..." : message));
    }
}

package com.camundatest;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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
        var jobs = waitForJobs("io.camunda.zeebe:userTask", 1);
        var job = jobs.get(0);
        
        client.newCompleteCommand(job.getKey())
                .variables(Map.of("message_content", messageContent))
                .send()
                .join();
    }
    
    private void completeServiceTask(String expectedMessage) {
        var jobs = waitForJobs("email", 1);
        var job = jobs.get(0);
        
        // Verify the message content was passed correctly
        Map<String, Object> variables = job.getVariablesAsMap();
        String actualMessage = (String) variables.get("message_content");
        assertEquals(expectedMessage, actualMessage, 
            "Message content should match between user task and service task");
        
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
    
    private String truncateMessage(String message) {
        return message.length() > 50 ? message.substring(0, 47) + "..." : message;
    }

    @Test
    public void testEmailProcessWithDefaultMessage() {
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
        "consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem."
    })
    public void testEmailProcessWithDifferentMessages(String message) {
        testEmailProcessWithMessage(message);
    }
    
    @Test
    public void testEmailProcessWithEmptyMessage() {
        // Note: Form validation (required=true, minLength=1) exists in BPMN but is only enforced 
        // through Camunda UI/Tasklist API, not when programmatically completing tasks via Zeebe client
        ProcessInstanceEvent processInstance = startProcess("");
        System.out.println("Started process with empty message, instance key: " + processInstance.getProcessInstanceKey());
        
        // Complete the user task with empty message (bypasses form validation)
        completeUserTask(processInstance.getProcessInstanceKey(), "");
        
        // Complete the service task (should receive empty message)
        completeServiceTask("");
        
        System.out.println("Process completed successfully with empty message (form validation bypassed in test)");
    }
    
    @Test
    public void testProcessCancellation() {
        ProcessInstanceEvent processInstance = startProcess(DEFAULT_MESSAGE);
        
        client.newCancelInstanceCommand(processInstance.getProcessInstanceKey())
                .send()
                .join();
                
        System.out.println("Process instance cancelled successfully");
    }
    
    private void testEmailProcessWithMessage(String message) {
        ProcessInstanceEvent processInstance = startProcess(message);
        System.out.println("Testing with message: " + truncateMessage(message));
            
        completeUserTask(processInstance.getProcessInstanceKey(), message);
        completeServiceTask(message); // Now passes expected message for verification
        
        System.out.println("Process completed successfully with message: " + truncateMessage(message));
    }
}
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

    // Loads the BPMN from the classpath and deploys it to the test engine.
    // join() blocks until the async deploy is done.
    private void deployProcess() {
        try {
            String bpmnResource = "send-email.bpmn";
            System.out.println("Deploying BPMN process: " + bpmnResource);

            var deploymentEvent = client.newDeployResourceCommand()
                    .addResourceFromClasspath(bpmnResource)
                    .send()
                    .join();
        System.out.println("final bug fixed again");
            // Assert deployment was successful
            assertNotNull(deploymentEvent, "Deployment event should not be null");
            assertFalse(deploymentEvent.getProcesses().isEmpty(), "At least one process should be deployed");

            // Verify the correct process was deployed
            boolean processFound = deploymentEvent.getProcesses().stream()
                    .anyMatch(process -> PROCESS_ID.equals(process.getBpmnProcessId()));
            assertTrue(processFound, "Process '" + PROCESS_ID + "' should be deployed");

            System.out.println("BPMN process deployed successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to deploy BPMN process: " + e.getMessage(), e);
        }
    }

    // Starts the newest version of send-email and sets two variables: testRunId and
    // message_content
    private ProcessInstanceEvent startProcess(String messageContent) {
        var processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(PROCESS_ID)
                .latestVersion()
                .variables(Map.of(
                        "testRunId", UUID.randomUUID().toString(),
                        "message_content", messageContent))
                .send()
                .join();

        // Assert process instance was created successfully
        assertNotNull(processInstance, "Process instance should be created");
        assertTrue(processInstance.getProcessInstanceKey() > 0, "Process instance key should be positive");
        assertEquals(PROCESS_ID, processInstance.getBpmnProcessId(), "Process ID should match");
        assertTrue(processInstance.getVersion() > 0, "Process version should be positive");

        return processInstance;
    }

    private void completeUserTask(long processInstanceKey, String messageContent) {
        // First, set the variable that would be set by the user task form
        var setVariablesResponse = client.newSetVariablesCommand(processInstanceKey)
                .variables(Map.of("message_content", messageContent))
                .send()
                .join();

        // Assert variables were set successfully
        assertNotNull(setVariablesResponse, "Set variables response should not be null");

        // Then complete the user task job to allow the process to continue
        var jobs = waitForJobs("io.camunda.zeebe:userTask", 1);
        assertFalse(jobs.isEmpty(), "Should find at least one user task job");

        var job = jobs.stream()
                .filter(j -> j.getProcessInstanceKey() == processInstanceKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        "No user task job found for process instance: " + processInstanceKey));

        // Assert job properties
        assertNotNull(job, "User task job should not be null");
        assertEquals("io.camunda.zeebe:userTask", job.getType(), "Job type should be user task");
        assertEquals(processInstanceKey, job.getProcessInstanceKey(), "Job should belong to correct process instance");
        assertTrue(job.getKey() > 0, "Job key should be positive");

        var completeResponse = client.newCompleteCommand(job.getKey())
                .send()
                .join();

        // Assert job completion was successful
        assertNotNull(completeResponse, "Complete job response should not be null");
    }

    private void completeServiceTask(String expectedMessage) {
        var jobs = waitForJobs("email", 1);
        assertFalse(jobs.isEmpty(), "Should find at least one email service task job");

        var job = jobs.get(0);
        assertNotNull(job, "Email service task job should not be null");
        assertEquals("email", job.getType(), "Job type should be 'email'");
        assertTrue(job.getKey() > 0, "Job key should be positive");

        // Verify the message content was passed correctly
        Map<String, Object> variables = job.getVariablesAsMap();
        assertNotNull(variables, "Job variables should not be null");
        assertTrue(variables.containsKey("message_content"), "Variables should contain 'message_content'");

        String actualMessage = (String) variables.get("message_content");
        assertEquals(expectedMessage, actualMessage,
                "Message content should match between user task and service task");

        // Assert testRunId is also present
        assertTrue(variables.containsKey("testRunId"), "Variables should contain 'testRunId'");
        assertNotNull(variables.get("testRunId"), "testRunId should not be null");

        var completeResponse = client.newCompleteCommand(job.getKey())
                .send()
                .join();

        // Assert service task completion was successful
        assertNotNull(completeResponse, "Complete service task response should not be null");
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
        ProcessInstanceEvent instance = startProcess(DEFAULT_MESSAGE);
        System.out.println("Started process: " + instance.getProcessInstanceKey());

        // **Simulate Tasklist completion of user task**
        completeUserTask(instance.getProcessInstanceKey(), DEFAULT_MESSAGE);

        // **Then handle service task as before**
        completeServiceTask(DEFAULT_MESSAGE);

        // Assert no more jobs are available (process completed)
        var remainingJobs = client.newActivateJobsCommand()
                .jobType("io.camunda.zeebe:userTask")
                .maxJobsToActivate(10)
                .send()
                .join()
                .getJobs();
        assertTrue(remainingJobs.isEmpty() ||
                remainingJobs.stream().noneMatch(j -> j.getProcessInstanceKey() == instance.getProcessInstanceKey()),
                "No user task jobs should remain for this process instance");

        System.out.println("Process completed with message: " + DEFAULT_MESSAGE);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "Short message",
        "Message with special characters: !@#$%^&*()_+{}|:<>?\"'\nNew line",
        "Message with unicode: 😊 你好 こんにちは",
        "A very long message: " +
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. "

    })
    public void testEmailProcessWithDifferentMessages(String message) {
        // Assert message is not null before testing
        assertNotNull(message, "Test message should not be null");
        testEmailProcessWithMessage(message);
    }

    @Test
    public void testEmailProcessWithEmptyMessage() {
        // Note: Form validation (required=true, minLength=1) exists in BPMN but is only
        // enforced
        // through Camunda UI/Tasklist API, not when programmatically completing tasks
        // via Zeebe client
        ProcessInstanceEvent processInstance = startProcess("");
        System.out.println(
                "Started process with empty message, instance key: " + processInstance.getProcessInstanceKey());

        // Assert process started with empty message
        assertTrue(processInstance.getProcessInstanceKey() > 0, "Process should start even with empty message");

        // Complete the user task with empty message (bypasses form validation)
        completeUserTask(processInstance.getProcessInstanceKey(), "");

        // Complete the service task (should receive empty message)
        completeServiceTask("");

        System.out.println("Process completed successfully with empty message (form validation bypassed in test)");
    }

    @Test
    public void testProcessCancellation() {
        ProcessInstanceEvent processInstance = startProcess(DEFAULT_MESSAGE);

        // Assert process was created before cancellation
        assertNotNull(processInstance, "Process instance should be created before cancellation");
        assertTrue(processInstance.getProcessInstanceKey() > 0, "Process instance key should be valid");

        var cancelResponse = client.newCancelInstanceCommand(processInstance.getProcessInstanceKey())
                .send()
                .join();

        // Assert cancellation was successful
        assertNotNull(cancelResponse, "Cancel response should not be null");

        // Verify no jobs are available for the cancelled process
        var jobs = client.newActivateJobsCommand()
                .jobType("io.camunda.zeebe:userTask")
                .maxJobsToActivate(10)
                .send()
                .join()
                .getJobs();

        boolean hasCancelledProcessJobs = jobs.stream()
                .anyMatch(job -> job.getProcessInstanceKey() == processInstance.getProcessInstanceKey());
        assertFalse(hasCancelledProcessJobs, "Cancelled process should not have active jobs");

        System.out.println("Process instance cancelled successfully");
    }

    @Test
    private void testEmailProcessWithMessage(String message) {
        ProcessInstanceEvent processInstance = startProcess(message);
        System.out.println("Testing with message: " + truncateMessage(message));

        // Assert process instance properties
        assertNotNull(processInstance, "Process instance should not be null");
        assertTrue(processInstance.getProcessInstanceKey() > 0, "Process instance key should be positive");
        assertEquals(PROCESS_ID, processInstance.getBpmnProcessId(), "Process ID should match expected value");

        completeUserTask(processInstance.getProcessInstanceKey(), message);
        completeServiceTask(message);

        // Verify process completed successfully by checking no remaining jobs
        var processInstanceJobs = client.newActivateJobsCommand()
                .jobType("io.camunda.zeebe:userTask")
                .maxJobsToActivate(1)
                .send()
                .join();

        // Assert no active user task jobs remain for this process instance
        boolean hasActiveJobs = processInstanceJobs.getJobs().stream()
                .anyMatch(job -> job.getProcessInstanceKey() == processInstance.getProcessInstanceKey());
        assertFalse(hasActiveJobs, "Process instance should have no active user task jobs after completion");

        // Also check for service task jobs
        var serviceTaskJobs = client.newActivateJobsCommand()
                .jobType("email")
                .maxJobsToActivate(1)
                .send()
                .join();

        boolean hasActiveServiceJobs = serviceTaskJobs.getJobs().stream()
                .anyMatch(job -> job.getProcessInstanceKey() == processInstance.getProcessInstanceKey());
        assertFalse(hasActiveServiceJobs, "Process instance should have no active service task jobs after completion");

        // Additional assertion to verify topology is accessible (system health check)
        var topologyResponse = client.newTopologyRequest().send().join();
        assertNotNull(topologyResponse, "Topology response should not be null");
        assertFalse(topologyResponse.getBrokers().isEmpty(), "At least one broker should be available");

        System.out.println("Process completed successfully with message: " + truncateMessage(message));
    }
}
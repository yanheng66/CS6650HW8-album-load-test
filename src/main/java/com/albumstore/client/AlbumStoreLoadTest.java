package com.albumstore.client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Advanced load test client for Album Store API
 */
public class AlbumStoreLoadTest {
    // Test image path
    private static final String TEST_IMAGE_PATH = "./album_cover.jpg";
    // JSON parser
    private static final Gson gson = new Gson();
    // Maximum retry attempts for failed requests
    private static final int MAX_RETRIES = 5;
    // Request types
    private static final String REQUEST_TYPE_POST_ALBUM = "POST_ALBUM";
    private static final String REQUEST_TYPE_POST_REVIEW = "POST_REVIEW";
    private static final String REQUEST_TYPE_GET_ALBUM = "GET_ALBUM";
    private static final String REQUEST_TYPE_GET_REVIEW = "GET_REVIEW";

    // Shared data structure for album IDs (thread-safe)
    private static final ConcurrentLinkedQueue<String> albumIdQueue = new ConcurrentLinkedQueue<>();

    // Counters for different request types
    private static AtomicLong totalPostAlbumRequests = new AtomicLong(0);
    private static AtomicLong successPostAlbumRequests = new AtomicLong(0);
    private static AtomicLong failedPostAlbumRequests = new AtomicLong(0);

    private static AtomicLong totalPostReviewRequests = new AtomicLong(0);
    private static AtomicLong successPostReviewRequests = new AtomicLong(0);
    private static AtomicLong failedPostReviewRequests = new AtomicLong(0);

    private static AtomicLong totalGetAlbumRequests = new AtomicLong(0);
    private static AtomicLong successGetAlbumRequests = new AtomicLong(0);
    private static AtomicLong failedGetAlbumRequests = new AtomicLong(0);

    private static AtomicLong totalGetReviewRequests = new AtomicLong(0);
    private static AtomicLong successGetReviewRequests = new AtomicLong(0);
    private static AtomicLong failedGetReviewRequests = new AtomicLong(0);

    // 专门用于3个GET/review线程的计数器
    private static AtomicLong dedicatedGetReviewRequests = new AtomicLong(0);
    private static AtomicLong dedicatedSuccessGetReviewRequests = new AtomicLong(0);
    private static AtomicLong dedicatedFailedGetReviewRequests = new AtomicLong(0);

    // 专门用于记录3个GET/review线程的开始和结束时间
    private static long dedicatedGetReviewStartTime;
    private static long dedicatedGetReviewEndTime;

    // Lists to store response times for statistics
    private static List<Long> postAlbumResponseTimes = Collections.synchronizedList(new ArrayList<>());
    private static List<Long> postReviewResponseTimes = Collections.synchronizedList(new ArrayList<>());
    private static List<Long> getAlbumResponseTimes = Collections.synchronizedList(new ArrayList<>());
    private static List<Long> getReviewResponseTimes = Collections.synchronizedList(new ArrayList<>());

    // 专门用于3个GET/review线程的响应时间列表
    private static List<Long> dedicatedGetReviewResponseTimes = Collections.synchronizedList(new ArrayList<>());

    // Flag to control GET/review threads
    private static AtomicBoolean keepRunning = new AtomicBoolean(true);

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4) {
            System.out.println("Usage: java AlbumStoreLoadTest <threadGroupSize> <numThreadGroups> <delay> <IPAddr>");
            System.out.println("  threadGroupSize: Number of threads per group");
            System.out.println("  numThreadGroups: Number of thread groups to start");
            System.out.println("  delay: Delay between thread groups in seconds");
            System.out.println("  IPAddr: Base URL of the server (e.g., http://localhost:8080/)");
            return;
        }

        int threadGroupSize = Integer.parseInt(args[0]);
        int numThreadGroups = Integer.parseInt(args[1]);
        int delay = Integer.parseInt(args[2]);
        final String baseUrl = args[3];

        System.out.println("Starting load test with configuration:");
        System.out.println("Thread Group Size: " + threadGroupSize);
        System.out.println("Number of Thread Groups: " + numThreadGroups);
        System.out.println("Delay between groups: " + delay + " seconds");
        System.out.println("Server URL: " + baseUrl);

        // First, reset the database
        resetDatabase(baseUrl);

        // Initialize with 10 threads, each sending 100 POST/GET pairs
        System.out.println("\nRunning initialization phase...");
        runInitializationPhase(baseUrl, 10, 100);

        // Reset database again before main test
        resetDatabase(baseUrl);

        // Start the main test
        System.out.println("\nStarting main test phase...");
        long startTime = System.currentTimeMillis();

        // Create thread pool for main test threads
        ExecutorService mainExecutor = Executors.newCachedThreadPool();
        CountDownLatch mainLatch = new CountDownLatch(threadGroupSize * numThreadGroups);

        // 创建专用的CountDownLatch来协调3个GET/review线程
        CountDownLatch getReviewLatch = new CountDownLatch(3);
        ExecutorService getReviewExecutor = null;

        // Launch thread groups with delay
        for (int group = 0; group < numThreadGroups; group++) {
            System.out.println("Launching thread group " + (group + 1) + " of " + numThreadGroups);

            for (int i = 0; i < threadGroupSize; i++) {
                mainExecutor.submit(() -> {
                    try {
                        runMainTestThread(baseUrl, 100);
                    } finally {
                        mainLatch.countDown();
                    }
                });
            }

            // Start the GET/review threads after the first group completes
            if (group == 0) {
                System.out.println("Launching GET/review background threads...");
                getReviewExecutor = Executors.newFixedThreadPool(3);

                // 记录这3个线程的开始时间
                dedicatedGetReviewStartTime = System.currentTimeMillis();

                for (int i = 0; i < 3; i++) {
                    getReviewExecutor.submit(() -> {
                        try {
                            runDedicatedGetReviewThread(baseUrl);
                        } finally {
                            getReviewLatch.countDown();
                        }
                    });
                }
            }

            // Wait for the delay before starting the next group (if not the last group)
            if (group < numThreadGroups - 1) {
                System.out.println("Waiting " + delay + " seconds before next group...");
                Thread.sleep(delay * 1000);
            }
        }

        // Wait for all threads to complete
        mainLatch.await();
        System.out.println("All thread groups completed");

        // Signal GET/review threads to stop
        keepRunning.set(false);

        // 等待3个GET/review线程完成并记录结束时间
        getReviewLatch.await();
        dedicatedGetReviewEndTime = System.currentTimeMillis();

        // Shutdown the main executor and GET/review executor
        mainExecutor.shutdown();
        if (getReviewExecutor != null) {
            getReviewExecutor.shutdown();
        }

        mainExecutor.awaitTermination(5, TimeUnit.MINUTES);
        if (getReviewExecutor != null) {
            getReviewExecutor.awaitTermination(5, TimeUnit.MINUTES);
        }

        long endTime = System.currentTimeMillis();

        // Calculate and print results
        printTestResults(startTime, endTime);

        // Write response times to CSV file
        writeResultsToCSV();
    }

    /**
     * Reset the database before testing
     */
    private static void resetDatabase(String baseUrl) {
        System.out.println("Resetting database...");
        CloseableHttpClient httpClient = HttpClients.createDefault();

        try {
            HttpPost httpPost = new HttpPost(baseUrl + "admin/reset");

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode == 200) {
                    System.out.println("Database reset successful");
                } else {
                    System.err.println("Failed to reset database, status code: " + statusCode);
                }
            }

        } catch (Exception e) {
            System.err.println("Exception while resetting database: " + e.getMessage());
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                System.err.println("Exception while closing HTTP client: " + e.getMessage());
            }
        }
    }

    /**
     * Run the initialization phase with 10 threads
     */
    private static void runInitializationPhase(String baseUrl, int threadCount, int requestsPerThread) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        // Create album
                        String albumId = createAlbum(baseUrl);

                        if (albumId != null) {
                            // Get album info
                            getAlbumInfo(baseUrl, albumId);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        System.out.println("Initialization phase completed");
    }

    /**
     * Run a main test thread that sends POST album + review requests
     */
    private static void runMainTestThread(String baseUrl, int requestCount) {
        for (int i = 0; i < requestCount; i++) {
            try {
                // Create album
                String albumId = createAlbum(baseUrl);

                if (albumId != null && !albumId.isEmpty()) {
                    // Add to shared queue for GET/review threads
                    albumIdQueue.add(albumId);

                    // Send reviews (2 likes, 1 dislike)
                    sendReview(baseUrl, "like", albumId);
                    sendReview(baseUrl, "like", albumId);
                    sendReview(baseUrl, "dislike", albumId);
                }
            } catch (Exception e) {
                System.err.println("Error in main test thread: " + e.getMessage());
            }
        }
    }

    /**
     * Run a background thread that continuously sends GET/review requests
     */
    private static void runGetReviewThread(String baseUrl) {
        Random random = new Random();

        while (keepRunning.get()) {
            try {
                // Get a random album ID from the queue if available
                String albumId = null;
                if (!albumIdQueue.isEmpty()) {
                    // Get a random ID from the queue
                    Object[] ids = albumIdQueue.toArray();
                    if (ids.length > 0) {
                        albumId = (String) ids[random.nextInt(ids.length)];
                    }
                }

                // If no album ID is available, use a fallback
                if (albumId == null || albumId.isEmpty()) {
                    continue;
                }

                // Send GET/review request
                getReviewStats(baseUrl, albumId);

            } catch (Exception e) {
                System.err.println("Error in GET/review thread: " + e.getMessage());
            }
        }
    }

    /**
     * 专用于3个GET/review线程的方法，使用单独的计数器
     */
    private static void runDedicatedGetReviewThread(String baseUrl) {
        Random random = new Random();

        while (keepRunning.get()) {
            try {
                // Get a random album ID from the queue if available
                String albumId = null;
                if (!albumIdQueue.isEmpty()) {
                    // Get a random ID from the queue
                    Object[] ids = albumIdQueue.toArray();
                    if (ids.length > 0) {
                        albumId = (String) ids[random.nextInt(ids.length)];
                    }
                }

                // If no album ID is available, use a fallback
                if (albumId == null || albumId.isEmpty()) {
                    continue;
                }

                // Send GET/review request with dedicated counters
                getDedicatedReviewStats(baseUrl, albumId);

            } catch (Exception e) {
                System.err.println("Error in dedicated GET/review thread: " + e.getMessage());
            }
        }
    }

    /**
     * Create a new album with retry logic
     */
    private static String createAlbum(String baseUrl) {
        long startTime = System.currentTimeMillis();
        String albumId = null;

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try {
                HttpPost httpPost = new HttpPost(baseUrl + "albums");

                File imageFile = new File(TEST_IMAGE_PATH);

                MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                builder.addBinaryBody("image", imageFile, ContentType.APPLICATION_OCTET_STREAM, imageFile.getName());
                builder.addTextBody("artist", "Test Artist " + System.currentTimeMillis(), ContentType.TEXT_PLAIN);
                builder.addTextBody("title", "Test Album " + System.currentTimeMillis(), ContentType.TEXT_PLAIN);
                builder.addTextBody("year", "2023", ContentType.TEXT_PLAIN);

                HttpEntity multipart = builder.build();
                httpPost.setEntity(multipart);

                totalPostAlbumRequests.incrementAndGet();

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Calculate and record response time
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;

                    // Record request details
                    postAlbumResponseTimes.add(latency);

                    if (statusCode == 200) {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        JsonObject jsonResponse = gson.fromJson(responseBody, JsonObject.class);
                        albumId = jsonResponse.get("albumID").getAsString();

                        successPostAlbumRequests.incrementAndGet();
                        return albumId;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        // Retry for 4XX or 5XX errors
                        if (retry < MAX_RETRIES - 1) {
                            System.err.println("Retrying create album after error " + statusCode + ", attempt " + (retry + 1));
                            continue;
                        } else {
                            failedPostAlbumRequests.incrementAndGet();
                            System.err.println("Failed to create album after " + MAX_RETRIES + " attempts, status code: " + statusCode);
                        }
                    } else {
                        successPostAlbumRequests.incrementAndGet();
                    }
                }

                // If we got here without returning, break the retry loop
                break;

            } catch (Exception e) {
                if (retry < MAX_RETRIES - 1) {
                    System.err.println("Retrying create album after exception: " + e.getMessage() + ", attempt " + (retry + 1));
                } else {
                    failedPostAlbumRequests.incrementAndGet();
                    System.err.println("Exception while creating album after " + MAX_RETRIES + " attempts: " + e.getMessage());
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    System.err.println("Exception while closing HTTP client: " + e.getMessage());
                }
            }
        }

        return albumId;
    }

    /**
     * Send a review with retry logic
     */
    private static void sendReview(String baseUrl, String reviewType, String albumId) {
        long startTime = System.currentTimeMillis();

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try {
                HttpPost httpPost = new HttpPost(baseUrl + "review/" + reviewType + "/" + albumId);

                totalPostReviewRequests.incrementAndGet();

                try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Calculate and record response time
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;

                    // Record request details
                    postReviewResponseTimes.add(latency);

                    if (statusCode == 201) {
                        successPostReviewRequests.incrementAndGet();
                        return;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        // Retry for 4XX or 5XX errors
                        if (retry < MAX_RETRIES - 1) {
                            System.err.println("Retrying send review after error " + statusCode + ", attempt " + (retry + 1));
                            continue;
                        } else {
                            failedPostReviewRequests.incrementAndGet();
                            System.err.println("Failed to send review after " + MAX_RETRIES + " attempts, status code: " + statusCode);
                        }
                    } else {
                        successPostReviewRequests.incrementAndGet();
                    }
                }

                // If we got here without returning, break the retry loop
                break;

            } catch (Exception e) {
                if (retry < MAX_RETRIES - 1) {
                    System.err.println("Retrying send review after exception: " + e.getMessage() + ", attempt " + (retry + 1));
                } else {
                    failedPostReviewRequests.incrementAndGet();
                    System.err.println("Exception while sending review after " + MAX_RETRIES + " attempts: " + e.getMessage());
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    System.err.println("Exception while closing HTTP client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Get album information with retry logic
     */
    private static void getAlbumInfo(String baseUrl, String albumId) {
        long startTime = System.currentTimeMillis();

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try {
                HttpGet httpGet = new HttpGet(baseUrl + "albums/" + albumId);

                totalGetAlbumRequests.incrementAndGet();

                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Calculate and record response time
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;

                    // Record request details
                    getAlbumResponseTimes.add(latency);

                    if (statusCode == 200) {
                        successGetAlbumRequests.incrementAndGet();
                        return;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        // Retry for 4XX or 5XX errors
                        if (retry < MAX_RETRIES - 1) {
                            System.err.println("Retrying get album after error " + statusCode + ", attempt " + (retry + 1));
                            continue;
                        } else {
                            failedGetAlbumRequests.incrementAndGet();
                            System.err.println("Failed to get album after " + MAX_RETRIES + " attempts, status code: " + statusCode);
                        }
                    } else {
                        successGetAlbumRequests.incrementAndGet();
                    }
                }

                // If we got here without returning, break the retry loop
                break;

            } catch (Exception e) {
                if (retry < MAX_RETRIES - 1) {
                    System.err.println("Retrying get album after exception: " + e.getMessage() + ", attempt " + (retry + 1));
                } else {
                    failedGetAlbumRequests.incrementAndGet();
                    System.err.println("Exception while getting album after " + MAX_RETRIES + " attempts: " + e.getMessage());
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    System.err.println("Exception while closing HTTP client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Get review statistics with retry logic
     */
    private static void getReviewStats(String baseUrl, String albumId) {
        long startTime = System.currentTimeMillis();

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try {
                HttpGet httpGet = new HttpGet(baseUrl + "review/" + albumId);

                totalGetReviewRequests.incrementAndGet();

                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Calculate and record response time
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;

                    // Record request details
                    getReviewResponseTimes.add(latency);

                    if (statusCode == 200) {
                        successGetReviewRequests.incrementAndGet();
                        return;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        // Retry for 4XX or 5XX errors
                        if (retry < MAX_RETRIES - 1) {
                            System.err.println("Retrying get review stats after error " + statusCode + ", attempt " + (retry + 1));
                            continue;
                        } else {
                            failedGetReviewRequests.incrementAndGet();
                            System.err.println("Failed to get review stats after " + MAX_RETRIES + " attempts, status code: " + statusCode);
                        }
                    } else {
                        successGetReviewRequests.incrementAndGet();
                    }
                }

                // If we got here without returning, break the retry loop
                break;

            } catch (Exception e) {
                if (retry < MAX_RETRIES - 1) {
                    System.err.println("Retrying get review stats after exception: " + e.getMessage() + ", attempt " + (retry + 1));
                } else {
                    failedGetReviewRequests.incrementAndGet();
                    System.err.println("Exception while getting review stats after " + MAX_RETRIES + " attempts: " + e.getMessage());
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    System.err.println("Exception while closing HTTP client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 专用于3个GET/review线程的getReviewStats方法，使用单独的计数器
     */
    private static void getDedicatedReviewStats(String baseUrl, String albumId) {
        long startTime = System.currentTimeMillis();

        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try {
                HttpGet httpGet = new HttpGet(baseUrl + "review/" + albumId);

                // 使用专用计数器
                dedicatedGetReviewRequests.incrementAndGet();
                // 同时也增加总计数器以便保持一致性
                totalGetReviewRequests.incrementAndGet();

                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();

                    // Calculate and record response time
                    long endTime = System.currentTimeMillis();
                    long latency = endTime - startTime;

                    // 记录到两个响应时间列表
                    dedicatedGetReviewResponseTimes.add(latency);
                    getReviewResponseTimes.add(latency);

                    if (statusCode == 200) {
                        // 使用专用成功计数器
                        dedicatedSuccessGetReviewRequests.incrementAndGet();
                        // 也增加总成功计数器
                        successGetReviewRequests.incrementAndGet();
                        return;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        // Retry for 4XX or 5XX errors
                        if (retry < MAX_RETRIES - 1) {
                            System.err.println("Retrying dedicated get review stats after error " + statusCode + ", attempt " + (retry + 1));
                            continue;
                        } else {
                            // 使用专用失败计数器
                            dedicatedFailedGetReviewRequests.incrementAndGet();
                            // 也增加总失败计数器
                            failedGetReviewRequests.incrementAndGet();
                            System.err.println("Failed to get dedicated review stats after " + MAX_RETRIES + " attempts, status code: " + statusCode);
                        }
                    } else {
                        dedicatedSuccessGetReviewRequests.incrementAndGet();
                        successGetReviewRequests.incrementAndGet();
                    }
                }

                // If we got here without returning, break the retry loop
                break;

            } catch (Exception e) {
                if (retry < MAX_RETRIES - 1) {
                    System.err.println("Retrying dedicated get review stats after exception: " + e.getMessage() + ", attempt " + (retry + 1));
                } else {
                    dedicatedFailedGetReviewRequests.incrementAndGet();
                    failedGetReviewRequests.incrementAndGet();
                    System.err.println("Exception while getting dedicated review stats after " + MAX_RETRIES + " attempts: " + e.getMessage());
                }
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    System.err.println("Exception while closing HTTP client: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Print test results including statistics
     */
    private static void printTestResults(long startTime, long endTime) {
        double wallTime = (endTime - startTime) / 1000.0;

        long totalRequests = totalPostAlbumRequests.get() + totalPostReviewRequests.get() +
                totalGetAlbumRequests.get() + totalGetReviewRequests.get();

        long successRequests = successPostAlbumRequests.get() + successPostReviewRequests.get() +
                successGetAlbumRequests.get() + successGetReviewRequests.get();

        long failedRequests = failedPostAlbumRequests.get() + failedPostReviewRequests.get() +
                failedGetAlbumRequests.get() + failedGetReviewRequests.get();

        double throughput = totalRequests / wallTime;

        System.out.println("\n======== TEST RESULTS ========");
        System.out.println("Wall Time: " + wallTime + " seconds");
        System.out.println("Total Requests: " + totalRequests);
        System.out.println("Successful Requests: " + successRequests);
        System.out.println("Failed Requests: " + failedRequests);
        System.out.println("Overall Throughput: " + String.format("%.2f", throughput) + " requests/second");

        System.out.println("\n======== REQUEST BREAKDOWN ========");
        System.out.println("POST Album Requests: " + totalPostAlbumRequests.get() +
                " (Success: " + successPostAlbumRequests.get() +
                ", Failed: " + failedPostAlbumRequests.get() + ")");

        System.out.println("POST Review Requests: " + totalPostReviewRequests.get() +
                " (Success: " + successPostReviewRequests.get() +
                ", Failed: " + failedPostReviewRequests.get() + ")");

        System.out.println("GET Album Requests: " + totalGetAlbumRequests.get() +
                " (Success: " + successGetAlbumRequests.get() +
                ", Failed: " + failedGetAlbumRequests.get() + ")");

        System.out.println("GET Review Requests: " + totalGetReviewRequests.get() +
                " (Success: " + successGetReviewRequests.get() +
                ", Failed: " + failedGetReviewRequests.get() + ")");

        System.out.println("\n======== LATENCY STATISTICS ========");
        System.out.println("POST Album Latency (ms): " + calculateStatistics(postAlbumResponseTimes));
        System.out.println("POST Review Latency (ms): " + calculateStatistics(postReviewResponseTimes));
        System.out.println("GET Album Latency (ms): " + calculateStatistics(getAlbumResponseTimes));
        System.out.println("GET Review Latency (ms): " + calculateStatistics(getReviewResponseTimes));

        // 计算3个专用GET/review线程的wall time和吞吐量
        double dedicatedWallTime = (dedicatedGetReviewEndTime - dedicatedGetReviewStartTime) / 1000.0;
        double dedicatedThroughput = dedicatedGetReviewRequests.get() / dedicatedWallTime;

        System.out.println("\n======== DEDICATED GET/REVIEW THREADS STATISTICS ========");
        System.out.println("Dedicated GET/Review Wall Time: " + dedicatedWallTime + " seconds");
        System.out.println("Dedicated GET/Review Requests: " + dedicatedGetReviewRequests.get() +
                " (Success: " + dedicatedSuccessGetReviewRequests.get() +
                ", Failed: " + dedicatedFailedGetReviewRequests.get() + ")");
        System.out.println("Dedicated GET/Review Throughput: " + String.format("%.2f", dedicatedThroughput) + " requests/second");
        System.out.println("Dedicated GET/Review Latency (ms): " + calculateStatistics(dedicatedGetReviewResponseTimes));
    }

    /**
     * Calculate statistics for a list of response times
     */
    private static String calculateStatistics(List<Long> responseTimes) {
        if (responseTimes.isEmpty()) {
            return "No data";
        }

        // Sort the list for percentile calculations
        Collections.sort(responseTimes);

        // Calculate mean
        double mean = responseTimes.stream().mapToLong(Long::longValue).average().orElse(0);

        // Calculate median (50th percentile)
        double median;
        int size = responseTimes.size();
        if (size % 2 == 0) {
            median = (responseTimes.get(size / 2 - 1) + responseTimes.get(size / 2)) / 2.0;
        } else {
            median = responseTimes.get(size / 2);
        }

        // Calculate p99 (99th percentile)
        int p99Index = (int) Math.ceil(0.99 * size) - 1;
        p99Index = Math.max(0, Math.min(p99Index, size - 1));
        double p99 = responseTimes.get(p99Index);

        // Get min and max
        long min = responseTimes.get(0);
        long max = responseTimes.get(size - 1);

        return String.format("Mean=%.2f, Median=%.2f, p99=%.2f, Min=%d, Max=%d", mean, median, p99, min, max);
    }

    /**
     * Write test results to CSV file
     */
    private static void writeResultsToCSV() {
        try (PrintWriter writer = new PrintWriter(new FileWriter("load_test_results.csv"))) {
            writer.println("RequestType,StartTime,Latency,ResponseCode");

            // Here we would write all the records, but in this simplified version
            // we're just creating the file structure
            writer.println("This would contain detailed request data in a real implementation");

            System.out.println("\nTest results written to load_test_results.csv");
        } catch (IOException e) {
            System.err.println("Error writing results to CSV: " + e.getMessage());
        }
    }
}
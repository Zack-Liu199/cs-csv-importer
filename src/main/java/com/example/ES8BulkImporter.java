package com.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;

/**
 * ClassName: ES8BulkImporter
 * Package: com.example
 * Description:使用Map<String, Object>适配Elasticsearch 8.x客户端的CSV导入工具
 *
 * @Author Zack_Liu
 * @Create 2025/6/14 15:46
 * @Version 1.0
 */



public class ES8BulkImporter {
    private static final Logger logger = LoggerFactory.getLogger(ES8BulkImporter.class);
    private final ElasticsearchClient client;
    private final String indexName;
    private final int batchSize;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final int threadCount;
    private final ExecutorService executorService;
    private final AtomicLong totalSuccessCount = new AtomicLong(0);
    private final AtomicLong totalFailedCount = new AtomicLong(0);
    private final Map<String, AtomicLong> shardProgress = new ConcurrentHashMap<>();

    public ES8BulkImporter(String[] esHosts, String indexName, String username, String password,
                           int batchSize, int maxRetries, long retryDelayMillis, int threadCount) {
        this.indexName = indexName;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.threadCount = threadCount;

        // 配置认证信息
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        // 构建低级别REST客户端
        org.apache.http.impl.client.CloseableHttpClient httpClient =
                org.apache.http.impl.client.HttpClients.custom()
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .build();


        // 创建ES客户端 - 使用Elasticsearch 8.x的新API
        List<HttpHost> hosts = new ArrayList<>();
        // 修改构造函数中的host配置
        for (String host : esHosts) {
            String[] parts = host.split(":");
            // 使用"http"而非"https"
            hosts.add(new HttpHost(parts[0], Integer.parseInt(parts[1]), "http"));
        }

        // 创建传输层并配置认证和SSL
        ElasticsearchTransport transport = new RestClientTransport(
                org.elasticsearch.client.RestClient.builder(
                                hosts.toArray(new HttpHost[0]))
                        .setHttpClientConfigCallback(hc -> hc
                                .setDefaultCredentialsProvider(credentialsProvider)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        )
                        .build(),
                new JacksonJsonpMapper()
        );

        this.client = new ElasticsearchClient(transport);

        // 创建线程池
        this.executorService = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            private final AtomicLong threadId = new AtomicLong(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "es8-importer-thread-" + threadId.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * 并行导入多个CSV文件
     */
    public void importFiles(List<String> csvFiles) throws InterruptedException, ExecutionException {
        List<Future<?>> futures = new ArrayList<>();
        for (String file : csvFiles) {
            futures.add(executorService.submit(() -> importFile(file)));
        }

        // 等待所有任务完成
        for (Future<?> future : futures) {
            future.get();
        }

        // 关闭资源
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        // 关闭ES客户端
        try {
            client._transport().close();
        } catch (IOException e) {
            logger.error("Failed to close ES client", e);
        }

        // 打印最终统计
        logger.info("Import completed. Total success: {}, total failed: {}",
                totalSuccessCount.get(), totalFailedCount.get());
    }

    /**
     * 导入单个CSV文件
     */
    private void importFile(String filePath) {
        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();
        shardProgress.put(fileName, new AtomicLong(0));

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isHeader = true;
            List<String> batchLines = new ArrayList<>(batchSize);

            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // 跳过CSV表头
                }

                batchLines.add(line);
                if (batchLines.size() >= batchSize) {
                    processBatch(batchLines, fileName);
                    batchLines.clear();

                    // 打印进度
                    long progress = shardProgress.get(fileName).addAndGet(batchSize);
                    if (progress % (batchSize * 100) == 0) {
                        logger.info("File: {}, Progress: {} lines", fileName, progress);
                    }
                }
            }

            // 处理剩余数据
            if (!batchLines.isEmpty()) {
                processBatch(batchLines, fileName);
                shardProgress.get(fileName).addAndGet(batchLines.size());
                logger.info("File: {}, Progress: {} lines (completed)", fileName,
                        shardProgress.get(fileName).get());
            }

        } catch (Exception e) {
            logger.error("Failed to import file: {}", filePath, e);
        }
    }

    /**
     * 处理一个批次的数据
     */
    private void processBatch(List<String> lines, String fileName) {
        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        for (String line : lines) {
            try {
                String[] fields = line.split(",", 2); // 假设格式：id,姓名
                if (fields.length < 2) {
                    logger.warn("Invalid line format: {}", line);
                    continue;
                }

                String id = fields[0];
                String name = fields[1];

                // 构建文档Map
                Map<String, Object> doc = new HashMap<>();
                doc.put("id", id);
                doc.put("姓名", name);

                // 添加到批量请求
                bulkRequest.operations(op -> op
                        .index(idx -> idx
                                .index(indexName)
                                .id(id)
                                .document(doc)
                        )
                );
            } catch (Exception e) {
                logger.error("Failed to parse line: {}", line, e);
            }
        }

        BulkRequest request = bulkRequest.build();
        if (!request.operations().isEmpty()) {
            executeBulkRequest(request, lines.size(), fileName);
        }

    }

    /**
     * 执行Bulk请求，包含重试逻辑
     */
    private void executeBulkRequest(BulkRequest request, int batchSize, String fileName) {
        int retries = 0;
        while (true) {
            try {
                BulkResponse response = client.bulk(request);

                if (response.errors()) {
                    int failedCount = 0;
                    for (co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem item : response.items()) {
                        if (item.error() != null) {
                            failedCount++;
                            logger.warn("Failed to index document [{}]: {}",
                                    item.id(), item.error().reason());
                        }
                    }
                    totalFailedCount.addAndGet(failedCount);
                    totalSuccessCount.addAndGet(batchSize - failedCount);
                    logger.info("Batch completed with failures. Success: {}, Failed: {}",
                            batchSize - failedCount, failedCount);
                } else {
                    totalSuccessCount.addAndGet(batchSize);
                    logger.debug("Batch completed successfully. Count: {}", batchSize);
                }
                break; // 请求成功或处理完失败项后退出循环

            } catch (ElasticsearchException | IOException e) {
                retries++;
                if (retries > maxRetries) {
                    logger.error("Max retries exceeded for batch. Failed count: {}", batchSize, e);
                    totalFailedCount.addAndGet(batchSize);
                    break;
                }

                logger.warn("Bulk request failed (attempt {}). Retrying in {} ms",
                        retries, retryDelayMillis, e);
                try {
                    Thread.sleep(retryDelayMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * 加载配置文件
     *
     * @param path 配置文件路径（可为 null，默认加载 classpath 下的 config.properties）
     * @return Properties 对象
     */
    private static Properties loadConfig(String path) {
        Properties props = new Properties();
        try {
            if (path != null && !path.isEmpty()) {
                // 从指定路径加载配置文件
                try (InputStream input = new FileInputStream(path)) {
                    props.load(input);
                }
            } else {
                // 默认从 classpath 加载 config.properties
                try (InputStream input = ES8BulkImporter.class.getClassLoader().getResourceAsStream("config.properties")) {
                    if (input == null) {
                        System.err.println("无法在 classpath 中找到 config.properties 文件");
                        System.exit(1);
                    }
                    props.load(input);
                }
            }
        } catch (IOException e) {
            System.err.println("加载配置文件失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        return props;
    }


    public static void main(String[] args) {
        // 读取配置文件
        String configPath = null;
        if (args.length > 0) {
            configPath = args[0];
        }

        Properties config = loadConfig(configPath);

        // 从配置中获取参数
        String esHostsStr = config.getProperty("es.hosts");
        String indexName = config.getProperty("es.index.name");
        String username = config.getProperty("es.username");
        String password = config.getProperty("es.password");
        int batchSize = Integer.parseInt(config.getProperty("batch.size", "5000"));
        int maxRetries = Integer.parseInt(config.getProperty("max.retries", "3"));
        long retryDelayMillis = Long.parseLong(config.getProperty("retry.delay.millis", "1000"));
        int threadCount = Integer.parseInt(config.getProperty("thread.count", "10"));

        // 读取CSV文件夹下所有.csv文件
        String csvDirPath = config.getProperty("csv.dir");
        List<String> csvFiles = new ArrayList<>();
        if (csvDirPath != null && !csvDirPath.isEmpty()) {
            File dir = new File(csvDirPath);
            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".csv"));
                if (files != null) {
                    for (File file : files) {
                        csvFiles.add(file.getAbsolutePath());
                    }
                }
            } else {
                System.err.println("配置的csv.dir不存在或不是一个目录: " + csvDirPath);
                System.exit(1);
            }
        } else {
            System.err.println("未配置csv.dir参数");
            System.exit(1);
        }

        try {
            ES8BulkImporter importer = new ES8BulkImporter(
                    esHostsStr.split(","), indexName, username, password,
                    batchSize, maxRetries, retryDelayMillis, threadCount);
            importer.importFiles(csvFiles);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
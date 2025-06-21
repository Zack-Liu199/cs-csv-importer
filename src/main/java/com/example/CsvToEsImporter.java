package com.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.io.*;
import java.util.*;

public class CsvToEsImporter {
    private static Properties config;
    private static RestHighLevelClient client;
    private static BulkProcessor bulkProcessor;
    private static AtomicLong recordCount = new AtomicLong(0);

    public static void main(String[] args) {
        // 读取配置文件
        String configPath = args.length > 0 ? args[0] : "config.properties";
        config = loadConfig(configPath);

        // 从配置中获取参数（补充缺失的配置项）
        String esHostsStr = config.getProperty("es.hosts", "localhost:9200");
        String indexName = config.getProperty("es.index.name", "person_data");
        String username = config.getProperty("es.username");
        String password = config.getProperty("es.password");

        // 批量处理核心参数
        int batchSize = Integer.parseInt(config.getProperty("batch.size", "50000"));
        int flushInterval = Integer.parseInt(config.getProperty("flush.interval.seconds", "30"));
        long bulkSizeMB = Long.parseLong(config.getProperty("bulk.size.mb", "20"));
        int concurrentRequests = Integer.parseInt(config.getProperty("concurrent.requests", "2"));
        int maxRetries = Integer.parseInt(config.getProperty("max.retries", "3"));
        int retryDelayMillis = Integer.parseInt(config.getProperty("retry.delay.millis", "1000"));

        // 线程池配置（补充您已有的thread.count参数映射）
        int threadCount = Integer.parseInt(config.getProperty("thread.count", "5"));

        // 高级索引配置（可选）
        String refreshInterval = config.getProperty("index.refresh_interval", "1s");
        int numberOfShards = Integer.parseInt(config.getProperty("index.number_of_shards", "5"));
        int numberOfReplicas = Integer.parseInt(config.getProperty("index.number_of_replicas", "1"));

        // 读取CSV文件路径
        String csvDirPath = config.getProperty("csv.dir");
        List<Path> csvFiles = getCsvFiles(csvDirPath);
        if (csvFiles.isEmpty()) {
            System.err.println("未找到CSV文件，程序退出");
            System.exit(1);
        }

        try {
            // 初始化ES客户端和BulkProcessor
            client = createEsClient(esHostsStr, username, password);

            // 修正：传递所有参数给ensureIndexExists
            ensureIndexExists(client, indexName, numberOfShards, numberOfReplicas, refreshInterval);

            // 修正：获取BulkProcessor实例并保存到类变量
            bulkProcessor = initBulkProcessor(
                    client,
                    indexName,
                    batchSize,
                    flushInterval,
                    bulkSizeMB,
                    maxRetries,
                    retryDelayMillis,
                    concurrentRequests
            );
            int concurrentThreads = Integer.parseInt(config.getProperty("thread.count", "5"));
            // 多线程处理CSV文件（控制并发数避免内存压力）
            ExecutorService executor = Executors.newFixedThreadPool(concurrentThreads);
            for (Path csvFile : csvFiles) {
                executor.submit(() -> processCsvFile(csvFile, indexName));
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.MILLISECONDS);

            // 等待所有批量操作完成
            if (bulkProcessor != null) {
                bulkProcessor.awaitClose(30, java.util.concurrent.TimeUnit.SECONDS);
            }
            System.out.println("所有操作完成，共导入 " + recordCount.get() + " 条记录");

        } catch (Exception e) {
            System.err.println("导入过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 释放资源
            if (bulkProcessor != null) bulkProcessor.close();
            if (client != null) try { client.close(); } catch (IOException e) {}
        }
    }

    // 加载配置文件
    private static Properties loadConfig(String configPath) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(configPath)) {
            props.load(input);
            System.out.println("成功加载配置文件: " + configPath);
        } catch (IOException e) {
            System.err.println("配置文件加载失败: " + e.getMessage());
            e.printStackTrace();
        }
        return props;
    }

    // 创建ES客户端
    private static RestHighLevelClient createEsClient(String esHostsStr, String username, String password) {
        String[] hosts = esHostsStr.split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String[] hostPort = hosts[i].split(":");
            httpHosts[i] = new HttpHost(hostPort[0], hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 9200, "http");
        }

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (username != null && !username.isEmpty()) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));
        }

        return new RestHighLevelClient(
                RestClient.builder(httpHosts)
                        .setHttpClientConfigCallback(builder ->
                                builder.setDefaultCredentialsProvider(credentialsProvider))
        );
    }

    // 确保索引存在（优化：支持动态配置分片、副本和刷新间隔）
    private static void ensureIndexExists(RestHighLevelClient client, String indexName,
                                          int numberOfShards, int numberOfReplicas, String refreshInterval) throws IOException {
        if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);

            // 使用参数化配置替代硬编码
            request.settings("{\n" +
                    "  \"number_of_shards\": " + numberOfShards + ",\n" +
                    "  \"number_of_replicas\": " + numberOfReplicas + ",\n" +
                    "  \"refresh_interval\": \"" + refreshInterval + "\"\n" +  // 修正引号问题
                    "}", XContentType.JSON);

            request.mapping("{\n" +
                    "  \"properties\": {\n" +
                    "    \"id\": {\"type\": \"keyword\"},\n" +
                    "    \"姓名\": {\"type\": \"text\", \"analyzer\": \"ik_max_word\"}\n" +
                    "  }\n" +
                    "}", XContentType.JSON);

            client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println("索引创建成功: " + indexName);
        }
    }

    // 初始化BulkProcessor（优化：添加缺失的参数支持）
    private static BulkProcessor initBulkProcessor(RestHighLevelClient client, String indexName,
                                                   int batchSize, int flushInterval, long bulkSizeMB,
                                                   int maxRetries, int retryDelayMillis, int concurrentRequests) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("准备执行批量操作，待处理文档数: " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    System.err.println("批量操作部分失败: " + response.buildFailureMessage());
                } else {
                    recordCount.addAndGet(request.numberOfActions());
                    System.out.println("********成功导入 " + request.numberOfActions() + " 条记录，累计: " + recordCount.get() + "********");
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.err.println("批量操作失败，尝试重试: " + failure.getMessage());
            }
        };

        // 修复：返回BulkProcessor实例，而不是void
        return BulkProcessor.builder(
                        (request, bulkListener) -> client.bulkAsync(
                                request,
                                RequestOptions.DEFAULT,
                                bulkListener
                        ),
                        listener
                )
                .setBulkActions(batchSize)
                .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(concurrentRequests)  // 使用传入的参数
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(
                                TimeValue.timeValueMillis(retryDelayMillis),  // 使用传入的参数
                                maxRetries
                        )
                )
                .build();
    }

    // 处理单个CSV文件
    private static void processCsvFile(Path csvFile, String indexName) {
        System.out.println("==================开始处理文件: " + csvFile.getFileName() + "====================================" );
        long startTime = System.currentTimeMillis();

        try (ICsvMapReader mapReader = new CsvMapReader(
                new FileReader(csvFile.toFile()), CsvPreference.STANDARD_PREFERENCE)) {
            String[] header = mapReader.getHeader(true);
            if (header == null || header.length < 2) {
                System.err.println("文件表头无效: " + csvFile.getFileName());
                return;
            }

            Map<String, String> row;
            while ((row = mapReader.read(header)) != null) {
                // 转换为ES文档（显式处理类型）
                Map<String, Object> document = new HashMap<>();
                for (Map.Entry<String, String> entry : row.entrySet()) {
                    document.put(entry.getKey(), entry.getValue());
                }

                // 添加到BulkProcessor（自动控制内存）
                bulkProcessor.add(new IndexRequest(indexName)
                        .id(row.get("id"))
                        .source(document, XContentType.JSON));
            }

        } catch (Exception e) {
            System.err.println("处理文件失败: " + csvFile.getFileName() + ", 错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            System.out.printf("文件处理完成，耗时: %d ms\n", duration);
        }
    }

    // 获取CSV文件列表
    private static List<Path> getCsvFiles(String csvDirPath) {
        try {
            File dir = new File(csvDirPath);
            if (!dir.exists() || !dir.isDirectory()) {
                System.err.println("CSV目录不存在: " + csvDirPath);
                return Collections.emptyList();
            }
            return Files.walk(Paths.get(csvDirPath))
                    .filter(p -> p.toString().toLowerCase().endsWith(".csv"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.err.println("扫描CSV文件失败: " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
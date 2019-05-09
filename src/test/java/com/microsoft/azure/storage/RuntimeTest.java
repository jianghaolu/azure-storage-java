package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.BlobAccessConditions;
import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.PageBlobURL;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ReliableDownloadOptions;
import com.microsoft.azure.storage.blob.RequestRetryOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.azure.storage.blob.models.PageRange;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.NettyClient;
import com.microsoft.rest.v2.http.Slf4jLogger;
import com.microsoft.rest.v2.util.Base64Util;
import com.microsoft.rest.v2.util.FlowableUtil;
import groovy.lang.Tuple;
import io.reactivex.Flowable;
import io.reactivex.Single;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class RuntimeTest {

    private static String[][] storageAccounts = {
            {"storagesfrepro1", "k/LL6Zw8NgvuX/95DV4AJWdeUa1MEwIL5ZjqNzXG9+F7M5PeLYGCy+/+QCPdOqh6mz281uzJkitigKYOc1238g=="},
            {"storagesfrepro2", "b1ZRw+Fw/G5K6hw+q5h/Id44cfL7QYkEgAYli6gD71ko/PaovgxgyuHiiqnK6xc/4hqQAE2iEpsm23g1pLGAhQ=="},
            {"storagesfrepro3", "i94QG/C5s4PEUg30i1A3EZuvrBHf/iywUG+PTkF+911/Y2ykkpY/UQRP3eCGFDLTLO01rGtsUrvxAdd2kSjOMA=="},
            {"storagesfrepro4", "p3nCdCES+2zgj190ck1XL5qRnQy66jvkbWbnTywdHbqkNEMnGb70h+fAkyXqPbjh0QOLWOUaDg7KBQeEbOMFrg=="},
//            {"storagesfrepro5", "y4IsHIcjdnnJ15OVfHMgxjtD/mclAHu5gJCB+Qzc7IOlu8/tGalqqZrtEkzOg1G1HjxZvbzIqBzQy8L4GGH43w=="},
//            {"storagesfrepro6", "FB+f61l2p118fRodWspl9HOBqd2lwFpuX67pY4nuLBKQYdWWwewx5/gTlUAuLHKgMs6UCVwQE1kxBm+5OlV0rA=="},
//            {"storagesfrepro7", "TnTVWhl/U3VZozj61lNM7YeTjNLcgyMGUNobNR/cY8txXvdcJ+MjZ5MZizFq9fGuaiePatsFQ+UA0ROzJmf0kA=="},
//            {"storagesfrepro8", "zW3FtREo2L4Ds+2kSmHwtrGgi5NCu+HeTCDTW8tttxywavkqhLzShD+iV/7W46+vB5EiT2opQIZhnAqqAvNiFA=="},
//            {"storagesfrepro9", "ua2jFkXJwz/0YAbrsupSQhWtLFGQKUPzeULyd1ruObRwEPm9GUfz9MfvvtNJoj5PddviR6QgLMK8tBEmr92P0A=="},
//            {"storagesfrepro10", "Sk+lxx/r4/j5y8m0voZiZWvbFWNy5JNSGrlmnj0MC6oz+WXW1hFI+8FHgT8uapmkyOYmgMV9f3lA93sPDtl57w=="},
//            {"storagesfrepro11", "umoE9nFFpVG2vyn8XYmNdQCtfr9ghYBiUvtWEudqqNFQ9WrGdC3u65DNQ3bIyCMNW2cgEVpx7+TXrZIsdvg51g=="},
//            {"storagesfrepro12", "3R8WcxnOZWnk7nfeiK59QzimI/j2Y+/GaJJR0xEYQGU43EwNCGnF5zv8BZAab7wHkdHmpeKS2Iijz1gYZ67Afw=="},
//            {"storagesfrepro13", "0dmwWgaoPeasQtelws9KoWSODj8TsUHCR0A6ZZuHtWR2Be7KgCT5l1Wh4RMufkkXYBNrGgo5R0w1jQ09ZKC0qA=="},
//            {"storagesfrepro14", "TAp1VQ1dlw+o/L8CFw9vc6H+xaqrUDL4qe+O/Fi/jRC5AaOATOt0BjdXfuShkYc1lCcQBRhYb3ylc+pMCUYE1A=="},
//            {"storagesfrepro15", "haY8b/8fiUXes+YXOJh0D4CTK6lwnsF3GXTNivRkUThBs2fS3OH4MDeHrubnnggWetRykCiOxunj1bxGfeG96w=="},
//            {"storagesfrepro16", "1jBLCS1ipE5UW5vNJ2daH3FJF7K6k8DVTrR83YHfAktlpW5Kz3XcauExnuZcTAhrpppMRIkUr5WEwKY0Ps7pbQ=="},
//            {"storagesfrepro17", "n9VGF/OL4nNuXFCO/Nmb3kPY9Fu+Tn7jxIMerZ2vLX7B91uj/9RAl9/rPFsCP4l6JRqAEywmhIaDV4fYcSKyCA=="},
//            {"storagesfrepro18", "flE61ndmoxGeD73AFFc8Mbx4uXwHMCHrZp1gjLjAzgdV6aerS62c0l3aGzRQsf7OwmRVVr2pTGM0w0kVyFhN9Q=="},
//            {"storagesfrepro19", "3ydtA2zgKtz3f6RLOMgOeZDh6dQhI5m3z0KXjwqlvVqLCZAKFEQ0dApFGiF5TxZtDAg40M36Y5iLV2YE7xMgew=="},
//            {"storagesfrepro20", "BzyM3jbEGz+WhUvXb+lFqNLp6wxpU4SoavcNmXNMNGa6rliGJ0ynRxSNmg0OnNMR4QaAY3sWS6q1R3fbsl+H2Q=="},
//            {"storagesfrepro21", "FF0ama//pfNT0ZUAbzCOQLx7mLN4G3ESMGJv10S1omucTe6lpKcKJjFEr7uQsdaMlbY/vY8/74RLQn6ai/0l2Q=="},
//            {"storagesfrepro22", "FNRshpzCBEzCZWpgaWyLTmcjHRhn65rEGHGAn8xPVI7JkwxyztVsA7d8/dZjgZp+mx/izia2Qieu7dkrTvvBNA=="},
//            {"storagesfrepro23", "OkPJ6rAPz4tR7KcZDWIyaxPi+dyF4iQne8SMZeldZTkrQr1mrdNi5hAFtDqGdGxeMLcuAKAcuj3gzDIf4h949w=="},
//            {"storagesfrepro24", "LV34XVAfgvKsZwHWutI1McZPjD/0S0+TLqIkqNJpjBTB4sgxUHqArc5U8wCl1tBOLT0DHfX3gofQnmqNFASxEA=="},
//            {"storagesfrepro25", "CRCwfuo3vhDW4gCuI+Hg7TsCR5W49tUbWiHeeNKgB1+vZ7VgIvKEWxsm0PVYraz+bZa+U/Gzzyw8YQo4ZQATqA=="},
    };

    @Test
    public void testRuntime() throws Exception {
        final HttpClient client = new NettyClient.Factory().create(new HttpClientConfiguration(null));
        ExecutorService executorService = Executors.newFixedThreadPool(storageAccounts.length);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            for (String[] creds : storageAccounts) {
                futures.add(CompletableFuture.runAsync(() -> {
                    System.out.println("++++++++++Running account " + creds[0] + "++++++++++");
                    try {
                        testRuntimeIntern(creds[0], creds[1], client);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executorService));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } finally {
            executorService.shutdown();
        }
    }

    @Ignore("Not testing this")
    public void testGetNonExist() throws Exception {
        String accountName = "astorageaccountthatdoesntexist123";
        String accountKey = "k/LL6Zw8NgvuX/95DV4AJWdeUa1MEwIL5ZjqNzXG9+F7M5PeLYGCy+/+QCPdOqh6mz281uzJkitigKYOc1238g==";

        final HttpClient client = new NettyClient.Factory().create(new HttpClientConfiguration(null));
        // Use your Storage account's name and key to create a credential object; this is used to access your account.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        final HttpPipeline pipeline = StorageURL.createPipeline(
                credential,
                new PipelineOptions()
                        .withLogger(new Slf4jLogger(LoggerFactory.getLogger("runtime-test")).withMinimumLogLevel(HttpPipelineLogLevel.INFO))
                        .withClient(client));

        URL u = new URL(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName));
        ServiceURL serviceURL = new ServiceURL(u, pipeline);

        // Now you can use the ServiceURL to perform various container and blob operations.

        // This example shows several common operations just to get you started.

        /*
        Create a URL that references a to-be-created container in your Azure Storage account. This returns a
        ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL).
        Note that container names require lowercase.
         */

        ContainerURL containerURL = serviceURL.createContainerURL("acontainerthatdoesntexist");
        ContainerListBlobFlatSegmentResponse res = containerURL.listBlobsFlatSegment(null, new ListBlobsOptions()).blockingGet();
        for (BlobItem item : res.body().segment().blobItems()) {
            System.out.println(item.name());
        }
    }

    public void testRuntimeIntern(String accountName, String accountKey, HttpClient httpClient) throws Exception {
        // Use your Storage account's name and key to create a credential object; this is used to access your account.
        SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);

        /*
        Create a request pipeline that is used to process HTTP(S) requests and responses. It requires your accont
        credentials. In more advanced scenarios, you can configure telemetry, retry policies, logging, and other
        options. Also you can configure multiple pipelines for different scenarios.
         */
        final HttpPipeline pipeline = StorageURL.createPipeline(
                credential,
                new PipelineOptions()
                        .withLogger(new Slf4jLogger(LoggerFactory.getLogger("runtime-test")).withMinimumLogLevel(HttpPipelineLogLevel.INFO))
                        .withClient(httpClient)
                        .withRequestRetryOptions(new RequestRetryOptions(null, null, 300, null, null, null)));

        /*
        From the Azure portal, get your Storage account blob service URL endpoint.
        The URL typically looks like this:
         */
        URL u = new URL(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName));

        // Create a ServiceURL objet that wraps the service URL and a request pipeline.
        ServiceURL serviceURL = new ServiceURL(u, pipeline);

        // Now you can use the ServiceURL to perform various container and blob operations.

        // This example shows several common operations just to get you started.

        /*
        Create a URL that references a to-be-created container in your Azure Storage account. This returns a
        ContainerURL object that wraps the container's URL and a request pipeline (inherited from serviceURL).
        Note that container names require lowercase.
         */

        ContainerURL containerURL = serviceURL.createContainerURL("someothercontainer");
//        containerURL.create().subscribe();


        PageBlobURL pageBlobURL = containerURL.createPageBlobURL("page-01.vhd");

//        String filePath = "C:\\Users\\jianghlu\\Documents\\Virtual Machines\\benchmarkvm\\Virtual Hard Disks\\benchmarkvm.vhdx";
        String filePath = "C:\\Users\\jianghlu\\test-downloaded.vhd";
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(filePath));
        long blockSize = 64L * 1024;

        pageBlobURL.create(channel.size(), null, null, null, null, null)
                .flatMapPublisher(pbcr -> Flowable.range(0, (int) Math.ceil((double) channel.size() / blockSize))
                        .map(i -> i * blockSize)
                        .map(pos -> {
                            long count = pos + blockSize > channel.size() ? (channel.size() - pos) : blockSize;
                            PageRange pageRange = new PageRange().withStart(pos).withEnd(pos + count - 1);
                            return new Tuple(new Object[]{pageRange, FlowableUtil.readFile(channel, pos, count)});
                        })
                        .flatMap(t -> Single.defer(() -> pageBlobURL.uploadPages((PageRange) t.get(0), (Flowable<ByteBuffer>) t.get(1), null, null)).toFlowable(),
                                false))
                .blockingSubscribe();

//        TransferManager.downloadBlobToFile(AsynchronousFileChannel.open(Paths.get(filePath), StandardOpenOption.WRITE), pageBlobURL, BlobRange.DEFAULT, new TransferManagerDownloadFromBlobOptions(null, null, null, null, null));

//        AtomicLong bytes = new AtomicLong(0);
//        ExecutorService service = Executors.newFixedThreadPool(24);
//        List<CompletableFuture<Void>> futures = new ArrayList<>();
//        for (long i = 0; i < channel.size(); i+=blockSize) {
//            final long offset = i;
//            futures.add(CompletableFuture.runAsync(() -> {
//                try {
//                    bytes.addAndGet(pageBlobURL.download(new BlobRange().withOffset(offset).withCount(Math.min(blockSize, channel.size() - offset)), BlobAccessConditions.NONE, false, null)
//                            .flatMapPublisher(dr -> dr.body(new ReliableDownloadOptions()))
//                            .map(Buffer::remaining)
//                            .reduce((a, b) -> a + b)
//                            .toSingle().blockingGet());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }, service));
//        }
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
//        System.out.println("@@@ Downloaded bytes: " + bytes.get());
        /*
        Create a URL that references a to-be-created blob in your Azure Storage account's container.
        This returns a BlockBlobURL object that wraps the blob's URl and a request pipeline
        (inherited from containerURL). Note that blob names can be mixed case.
         */
//        BlockBlobURL blobURL = containerURL.createBlockBlobURL("block-out-tm.vhd");
//
//        String filePath = "D:\\out.vhd";
//        long blockSize = 4096L * 1024;
//        AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(filePath));
//
//        BlockIdGenerator generator = new BlockIdGenerator(channel.size(), blockSize);
//
//        Observable.range(0, (int) Math.ceil((double) channel.size() / blockSize))
//                .map(i -> {
//                    long pos = i * blockSize;
//                    long count = pos + blockSize > channel.size() ? (channel.size() - pos) : blockSize;
//                    return new Tuple(new Object[] { count, generator.getBlockId(), FlowableUtil.readFile(channel, pos, count) });
//                })
//                .flatMap(tuple -> {
//                    String blockId = (String) tuple.get(1);
//                    return Single.defer(() -> blobURL.stageBlock(blockId, (Flowable<ByteBuffer>) tuple.get(2), (long) tuple.get(0), null, null)).toObservable();
//                }, 24)
//                .doOnComplete(() -> channel.close())
//                .ignoreElements()
//                .andThen(Completable.defer(() -> blobURL.commitBlockList(generator.blockIds, null, null, null, null).ignoreElement()))
//                .blockingAwait();


//        TransferManager.uploadFileToBlockBlob(AsynchronousFileChannel.open(Paths.get(filePath)),
//                blobURL, BlockBlobURL.MAX_STAGE_BLOCK_BYTES, new TransferManagerUploadToBlockBlobOptions(null, null, null, null, 24))
//                .doOnSuccess(commonRestResponse -> {
//                    client.dumpChannelPool();
//                }).blockingGet();

        // Create the container on the service (with no metadata and no public access)
                        /*
                         Create the blob with string (plain text) content.
                         NOTE: It is imperative that the provided length matches the actual length exactly.
                         */
//                blobURL.upload(Flowable.just(ByteBuffer.wrap(data.getBytes())), data.length(),
//                        null, null, null, null)
//                .flatMap(blobsDownloadResponse ->
//                        // Download the blob's content.
//                        blobURL.download(null, null, false, null))
//                .flatMap(blobsDownloadResponse ->
//                        // Verify that the blob data round-tripped correctly.
//                        FlowableUtil.collectBytesInBuffer(blobsDownloadResponse.body(null))
//                                .doOnSuccess(byteBuffer -> {
//                                    if (byteBuffer.compareTo(ByteBuffer.wrap(data.getBytes())) != 0) {
//                                        throw new Exception("The downloaded data does not match the uploaded data.");
//                                    }
//                                }))
//                .flatMap(byteBuffer ->
//                        // Delete the blob we created earlier.
//                        blobURL.delete(null, null, null))
//                .flatMap(blobsDeleteResponse ->
//                        // Delete the container we created earlier.
//                        containerURL.delete(null, null))
//                /*
//                This will synchronize all the above operations. This is strongly discouraged for use in production as
//                it eliminates the benefits of asynchronous IO. We use it here to enable the sample to complete and
//                demonstrate its effectiveness.
//                 */
//                .blockingGet();
    }

    public static class BlockIdGenerator {
        private long total;
        private long blockSize;
        private long blocks;
        private int blockIdLength;
        private List<String> blockIds;

        public BlockIdGenerator(long total, long blockSize) {
            this.total = total;
            this.blockSize = blockSize;
            this.blocks = total / blockSize + 1;
            this.blockIdLength = (int) Math.floor(Math.log10(blocks)) + 1;
            this.blockIds = new ArrayList<>();
        }

        public synchronized String getBlockId() {
            String blockId = String.format("%0" + blockIdLength + "d", blockIds.size());
            blockId = new String(Base64Util.encode(blockId.getBytes()));
            blockIds.add(blockId);
            return blockId;
        }

        public List<String> blockIds() {
            return blockIds;
        }
    }
}

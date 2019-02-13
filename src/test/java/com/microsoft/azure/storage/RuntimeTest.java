package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.PageBlobURL;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.TransferManagerUploadToBlockBlobOptions;
import com.microsoft.azure.storage.blob.models.PageRange;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.microsoft.rest.v2.http.HttpPipelineLogLevel;
import com.microsoft.rest.v2.http.NettyClient;
import com.microsoft.rest.v2.http.Slf4jLogger;
import com.microsoft.rest.v2.util.Base64Util;
import com.microsoft.rest.v2.util.FlowableUtil;
import groovy.lang.Tuple;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class RuntimeTest {

    private static NettyClient client = (NettyClient) new NettyClient.Factory().create(new HttpClientConfiguration(null));

    @Test
    public void testRuntime() throws Exception {
        String accountName = System.getenv("ACCOUNT_NAME");
        String accountKey = System.getenv("ACCOUNT_KEY");

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
//                        .withLogger(new Slf4jLogger(LoggerFactory.getLogger("runtime-test")).withMinimumLogLevel(HttpPipelineLogLevel.INFO))
                        .withClient(client));

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


        PageBlobURL pageBlobURL = containerURL.createPageBlobURL("page.vhd");

//        String filePath = "C:\\Users\\jianghlu\\Documents\\Virtual Machines\\benchmarkvm\\Virtual Hard Disks\\benchmarkvm.vhdx";
        String filePath = "D:\\test-downloaded.vhd";
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(filePath));
        long blockSize = 4096L * 1024;

        pageBlobURL.create(channel.size(), null, null, null, null, null)
                .flatMapObservable(pbcr -> Observable.range(0, (int) Math.ceil((double) channel.size() / blockSize))
                        .map(i -> i * blockSize)
                        .map(pos -> {
                            long count = pos + blockSize > channel.size() ? (channel.size() - pos) : blockSize;
                            PageRange pageRange = new PageRange().withStart(pos).withEnd(pos + count - 1);
                            return new Tuple(new Object[]{pageRange, FlowableUtil.readFile(channel, pos, count)});
                        })
                        .flatMap(t -> Single.defer(() -> pageBlobURL.uploadPages((PageRange) t.get(0), (Flowable<ByteBuffer>) t.get(1), null, null)).toObservable(),
                                false, 24))
                .blockingSubscribe();

        /*
        Create a URL that references a to-be-created blob in your Azure Storage account's container.
        This returns a BlockBlobURL object that wraps the blob's URl and a request pipeline
        (inherited from containerURL). Note that blob names can be mixed case.
         */
        BlockBlobURL blobURL = containerURL.createBlockBlobURL("block-out-tm.vhd");
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

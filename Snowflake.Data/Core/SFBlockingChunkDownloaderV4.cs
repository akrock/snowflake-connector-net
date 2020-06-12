/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.IO.Compression;
using System.IO;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using System.Diagnostics;
using Newtonsoft.Json.Serialization;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class SFBlockingChunkDownloaderV4 : IChunkDownloader
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFBlockingChunkDownloaderV3>();
        private static IRestRequester restRequester = RestRequester.Instance;

        private readonly int _colCount;
        private readonly string qrmk;
        private readonly IReadOnlyList<ExecResponseChunk> chunkInfos;
        private readonly int prefetchSlot;
        private readonly Dictionary<string, string> chunkHeaders;
        // External cancellation token, used to stop download
        private readonly CancellationToken _cancellationToken;
        private readonly Task<IResultChunk>[] taskQueues;
        private int nextChunkToConsumeIndex;

        public SFBlockingChunkDownloaderV4(
            int colCount,
            List<ExecResponseChunk> chunkInfos,
            string qrmk,
            Dictionary<string, string> chunkHeaders,
            CancellationToken cancellationToken,
            SFBaseResultSet resultSet)
        {
            _colCount = colCount;
            this.qrmk = qrmk;
            this.chunkHeaders = chunkHeaders;
            _cancellationToken = cancellationToken;
            this.prefetchSlot = Math.Min(chunkInfos.Count, GetPrefetchThreads(resultSet));
            this.chunkInfos = chunkInfos;
            this.nextChunkToConsumeIndex = 0;
            this.taskQueues = new Task<IResultChunk>[prefetchSlot];

            for (int i = 0; i < prefetchSlot; i++)
            {
                taskQueues[i] = DownloadChunkAsync(i);
            }
        }

        private int GetPrefetchThreads(SFBaseResultSet resultSet)
        {
            Dictionary<SFSessionParameter, object> sessionParameters = resultSet.sfStatement.SfSession.ParameterMap;
            String val = (String) sessionParameters[SFSessionParameter.CLIENT_PREFETCH_THREADS];
            return Int32.Parse(val);
        }

        /*public Task<IResultChunk> GetNextChunkAsync()
        {
            return _downloadTasks.IsCompleted ? Task.FromResult<SFResultChunk>(null) : _downloadTasks.Take();
        }*/

        public Task<IResultChunk> GetNextChunkAsync()
        {
            var nextChunkToDownloadIndex = nextChunkToConsumeIndex + prefetchSlot;
            logger.InfoFmt("NextChunkToConsume: {0}, NextChunkToDownload: {1}",
                nextChunkToConsumeIndex, nextChunkToDownloadIndex);
            if (nextChunkToConsumeIndex < chunkInfos.Count)
            {
                Task<IResultChunk> chunk = taskQueues[nextChunkToConsumeIndex % prefetchSlot];

                if (nextChunkToDownloadIndex < chunkInfos.Count)
                {
                    taskQueues[nextChunkToDownloadIndex % prefetchSlot] = DownloadChunkAsync(nextChunkToDownloadIndex);
                }

                nextChunkToConsumeIndex++;
                return chunk;
            }
            else
            {
                return Task.FromResult<IResultChunk>(null);
            }
        }

        private async Task<IResultChunk> DownloadChunkAsync(int chunkIndex)
        {
            var chunkDescriptor = chunkInfos[chunkIndex];
            //logger.Info($"Start donwloading chunk #{downloadContext.chunkIndex}");
            S3DownloadRequest downloadRequest = new S3DownloadRequest()
            {
                Url = new UriBuilder(chunkDescriptor.url).Uri,
                qrmk = qrmk,
                // s3 download request timeout to one hour
                RestTimeout = TimeSpan.FromHours(1),
                HttpTimeout = Timeout.InfiniteTimeSpan, // Disable timeout for each request
                chunkHeaders = chunkHeaders
            };

            var chunk = new SFChunkV4(_colCount, chunkDescriptor.rowCount, chunkDescriptor.url, chunkIndex,
                chunkDescriptor.uncompressedSize);
            using (var httpResponse = await restRequester.GetAsync(downloadRequest, _cancellationToken)
                .ConfigureAwait(continueOnCapturedContext: false))
            using (Stream stream = await httpResponse.Content.ReadAsStreamAsync()
                .ConfigureAwait(continueOnCapturedContext: false))
            {
                ParseStreamIntoChunk(stream, chunk);
            }

            logger.InfoFmt("Succeed downloading chunk #{0}", chunk.ChunkIndex);
            return chunk;
        }
        
        /// <summary>
        ///     Content from s3 in format of 
        ///     ["val1", "val2", null, ...],
        ///     ["val3", "val4", null, ...],
        ///     ...
        ///     To parse it as a json, we need to preappend '[' and append ']' to the stream 
        /// </summary>
        /// <param name="content"></param>
        /// <param name="resultChunk"></param>
        private void ParseStreamIntoChunk(Stream content, IResultChunkBuilder resultChunk)
        {
            var parser = new ReusableChunkParser(content);
            parser.ParseChunk(resultChunk);
        }
    }

}

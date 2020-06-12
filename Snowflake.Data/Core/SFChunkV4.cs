/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Snowflake.Data.Core
{
    internal class SFChunkV4 : IResultChunk, IResultChunkBuilder
    {
        public int RowCount { get; }

        public int ColCount { get; }

        public string Url { get; }

        public int ChunkIndex { get; }

        private readonly BlockResult _data;

        internal SFChunkV4(
            int colCount,
            int rowCount,
            string url,
            int chunkIndex,
            int chunkSize)
        {
            ColCount = colCount;
            RowCount = rowCount;
            Url = url;
            ChunkIndex = chunkIndex;
            _data = new BlockResult(colCount, rowCount, chunkSize);
        }

        public int GetRowCount() => RowCount;

        public int GetChunkIndex() => ChunkIndex;

        public string ExtractCell(int rowIndex, int columnIndex)
        {
            if (_data == null)
            {
                throw new InvalidOperationException("Chunk was not sealed.");
            }

            return _data.Get(rowIndex * ColCount + columnIndex);
        }

        public void AddCell(byte[] bytes, int length)
        {
            _data.Add(bytes, length);
        }

        private class BlockResult : IDisposable
        {
            private const int SF_ARRAY_POOL_MIN_SIZE = 1_000;
            private const int SF_NULL_VALUE = -100;
            private const int blockLengthBits = 20;
            private const int blockLength = 1 << blockLengthBits;

            private const int metaBlockLengthBits = 15;
            private const int metaBlockLength = 1 << metaBlockLengthBits;

            private readonly byte[][] _data;
            private readonly DataIndex[][] _indices;
            private bool _isDisposed = false;
            private int nextIndex = 0;
            private int currentDataOffset = 0;

            public BlockResult(
                int rowCount,
                int colCount,
                int uncompressedSize)
            {
                int bytesNeeded = uncompressedSize - (rowCount * 2) - (rowCount * colCount);
                var blocksNeeded = getBlock(bytesNeeded - 1) + 1;
                var metaBlocksNeeded = getMetaBlock(rowCount * colCount - 1) + 1;

                _indices = Rent<DataIndex[]>(metaBlocksNeeded);
                for (var i = 0; i < metaBlocksNeeded; i++)
                {
                    _indices[i] = Rent<DataIndex>(metaBlockLength);
                }

                _data = Rent<byte[]>(blocksNeeded);
                for (var i = 0; i < blocksNeeded; i++)
                {
                    _data[i] = Rent<byte>(blockLength);
                }
            }

            private static T[] Rent<T>(int count)
            {
                // Small arrays incur more overhead from ArrayPool than it's probably worth.
                // See: https://adamsitnik.com/Array-Pool/
                if (count < SF_ARRAY_POOL_MIN_SIZE)
                {
                    return new T[count];
                }

                return ArrayPool<T>.Shared.Rent(count);
            }

            private static void Return<T>(T[] array)
            {
                if (array.Length > SF_ARRAY_POOL_MIN_SIZE)
                {
                    ArrayPool<T>.Shared.Return(array);
                }
            }

            public string Get(int index)
            {
                if (_isDisposed)
                {
                    //This shouldn't happen.
                    throw new ObjectDisposedException(nameof(BlockResult));
                }

                var idx = _indices[getMetaBlock(index)][getMetaBlockIndex(index)];
                if (idx.Length == SF_NULL_VALUE)
                {
                    return null;
                }

                int offset = idx.Offset;
                int length = idx.Length;

                // Handle data that spans multiple blocks
                if (spaceLeftOnBlock(offset) < length)
                {
                    int copied = 0;
                    byte[] cell = Rent<byte>(length);

                    try
                    {
                        while (copied < length)
                        {
                            int copySize
                                = Math.Min(length - copied, spaceLeftOnBlock(offset + copied));
                            Array.Copy(_data[getBlock(offset + copied)],
                                getBlockOffset(offset + copied),
                                cell, copied,
                                copySize);

                            copied += copySize;
                        }

                        return Encoding.UTF8.GetString(cell, 0, length);
                    }
                    finally
                    {
                        Return(cell);
                    }
                }

                //Data exists in a single block
                return Encoding.UTF8.GetString(_data[getBlock(offset)], getBlockOffset(offset), length);
            }

            public void Add(byte[] bytes, int length)
            {
                // store offset and length
                int block = getMetaBlock(nextIndex);
                int index = getMetaBlockIndex(nextIndex);

                if (bytes == null)
                {
                    _indices[block][index] = new DataIndex(0, SF_NULL_VALUE);
                }
                else
                {
                    int offset = currentDataOffset;
                    _indices[block][index] = new DataIndex(offset, length);

                    // copy bytes to data array
                    if (spaceLeftOnBlock(offset) < length)
                    {
                        int copied = 0;
                        while (copied < length)
                        {
                            int copySize = Math.Min(length - copied, spaceLeftOnBlock(offset + copied));
                            Array.Copy(bytes, copied,
                                _data[getBlock(offset + copied)],
                                getBlockOffset(offset + copied),
                                copySize);
                            copied += copySize;
                        }
                    }
                    else
                    {
                        Array.Copy(bytes, 0,
                            _data[getBlock(offset)],
                            getBlockOffset(offset), length);
                    }

                    currentDataOffset += length;
                }

                nextIndex++;
            }

            private static int spaceLeftOnBlock(int offset) => blockLength - getBlockOffset(offset);

            private static int getBlock(int offset) => offset >> blockLengthBits;

            private static int getBlockOffset(int offset) => offset & (blockLength - 1);

            private static int getMetaBlock(int index) => index >> metaBlockLengthBits;

            private static int getMetaBlockIndex(int index) => index & (metaBlockLength - 1);

            private readonly struct DataIndex
            {
                public int Offset { get; }
                public int Length { get; }

                public DataIndex(
                    int offset,
                    int length)
                {
                    Offset = offset;
                    Length = length;
                }
            }

            private void ReleaseResources()
            {
                if (_isDisposed)
                {
                    return;
                }

                lock (_data)
                {
                    if (_isDisposed)
                    {
                        return;
                    }

                    foreach (var idx in _indices)
                    {
                        Return(idx);
                    }

                    Return(_indices);

                    foreach (var block in _data)
                    {
                        Return(block);
                    }

                    Return(_data);
                }

                _isDisposed = true;
            }

            public void Dispose()
            {
                ReleaseResources();
                GC.SuppressFinalize(this);
            }

            ~BlockResult()
            {
                ReleaseResources();
            }
        }

        public void Dispose()
        {
            _data.Dispose();
        }
    }
}
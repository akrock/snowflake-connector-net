namespace Snowflake.Data.Core
{
    internal interface IResultChunkBuilder
    {
        void AddCell(byte[] bytes, int length);
    }
}
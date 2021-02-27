using K4os.Compression.LZ4;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Text;

namespace Uncodium.SimpleStore
{
    /// <summary></summary>
    public static class Utils
    {
        /// <summary>
        /// Encodes buffer with LZ4. Resulting buffer can be decoded with DecodeLz4SelfContained.
        /// </summary>
        public static ReadOnlySpan<byte> EncodeLz4SelfContained(ReadOnlySpan<byte> uncompressed)
        {
            var maxEncodedLength = LZ4Codec.MaximumOutputSize(uncompressed.Length);

            var target = new byte[4 + maxEncodedLength];

            var l = uncompressed.Length;
            target[0] = (byte)l; l >>= 8;
            target[1] = (byte)l; l >>= 8;
            target[2] = (byte)l; l >>= 8;
            target[3] = (byte)l;

            var encodedLength = LZ4Codec.Encode(uncompressed, target.AsSpan().Slice(4, maxEncodedLength));

            return target.AsSpan().Slice(0, 4 + encodedLength);
        }

        /// <summary>
        /// Decodes buffer encoded with EncodeLz4SelfContained.
        /// </summary>
        public static byte[] DecodeLz4SelfContained(ReadOnlySpan<byte> compressed)
        {
            int l = compressed[0];
            l += compressed[1] << 8;
            l += compressed[2] << 16;
            l += compressed[3] << 24;

            var uncompressed = new byte[l];
            var count = LZ4Codec.Decode(compressed.Slice(4), uncompressed);
            if (count != l) throw new Exception();
            return uncompressed;
        }
    }
}

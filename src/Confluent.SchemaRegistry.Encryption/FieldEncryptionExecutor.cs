
using System;
using Microsoft.Extensions.Caching.Memory;

namespace Confluent.SchemaRegistry.Encryption
{
    public class FieldEncryptionExecutor
    {
        public const string Type = "ENCRYPT";
        public const string HeaderNamePrefix = "encrypt";

        private static byte[] EmptyAAD = new byte[] { };
        private static byte Version = 0;
        private static int LengthVersion = 1;
        private static int LengthEncryptedDek = 4;
        private static int LengthKekId = 4;
        private static int LengthDekFormat = 4;

        private string kekId;
        private Cryptor keyCryptor;
        private Cryptor valueCryptor;
        private int cacheExpirySecs = 300;
        private int cacheSize = 1000;
        private bool keyDeterministic = true;
        private bool valueDeterministic = false;
        private readonly MemoryCache dekEncryptCache;
        private readonly MemoryCache dekDecryptCache;

        public FieldEncryptionExecutor()
        {
            dekEncryptCache = new MemoryCache(new MemoryCacheOptions
            {
                ExpirationScanFrequency = TimeSpan.FromSeconds(cacheExpirySecs),
                SizeLimit = cacheSize
            });
            dekDecryptCache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = cacheSize
            });
        }



    }
}

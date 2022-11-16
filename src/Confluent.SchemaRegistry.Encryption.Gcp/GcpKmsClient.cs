using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Kms.V1;
using Google.Protobuf;

namespace Confluent.SchemaRegistry.Encryption.Gcp
{
    public class GcpKmsClient : IKmsClient
    {
        public static readonly string Prefix = "gcp-kms://";

        private KeyManagementServiceClient kmsClient;
        private string keyId;
        private CryptoKeyName keyName;
        
        public string KekId { get; }
        
        public GcpKmsClient(string kekId)
        {
            KekId = kekId;
            
            if (!kekId.StartsWith(Prefix)) {
              throw new ArgumentException(string.Format($"key URI must start with {Prefix}"));
            }
            keyId = KekId.Substring(Prefix.Length);
            keyName = CryptoKeyName.Parse(keyId);
            kmsClient = KeyManagementServiceClient.Create();
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var result = await kmsClient.EncryptAsync(keyName, ByteString.CopyFrom(plaintext));
            return result.Ciphertext.ToByteArray();
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var result = await kmsClient.DecryptAsync(keyId, ByteString.CopyFrom(ciphertext));
            return result.Plaintext.ToByteArray();
        }
    }
}
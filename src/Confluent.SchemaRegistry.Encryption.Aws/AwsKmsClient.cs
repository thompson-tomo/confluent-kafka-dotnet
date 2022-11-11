using System.IO;
using System.Threading.Tasks;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;

namespace Confluent.SchemaRegistry.Encryption.Aws
{
    public class AwsKmsClient : IKmsClient
    {
        public static readonly string Prefix = "aws-kms://";

        private AmazonKeyManagementServiceClient kmsClient;
        
        public string KmsKeyId { get; set; }
        public string AccessKeyId { get; set; }
        public string SecretAccessKey { get; set; }
        
        public AwsKmsClient(string kmsKeyId, string accessKeyId, string secretAccessKey)
        {
            KmsKeyId = kmsKeyId;
            AccessKeyId = accessKeyId;
            SecretAccessKey = secretAccessKey;
            kmsClient = new AmazonKeyManagementServiceClient(accessKeyId, secretAccessKey);
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            using var dataStream = new MemoryStream(plaintext);
            var request = new EncryptRequest
            {
                KeyId = KmsKeyId,
                Plaintext = dataStream
            };
            var response = await kmsClient.EncryptAsync(request);
            return response.CiphertextBlob.ToArray();
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            using var dataStream = new MemoryStream(ciphertext);
            var request = new DecryptRequest
            {
                KeyId = KmsKeyId,
                CiphertextBlob = dataStream
            };
            var response = await kmsClient.DecryptAsync(request);
            return response.Plaintext.ToArray();
        }
    }
}
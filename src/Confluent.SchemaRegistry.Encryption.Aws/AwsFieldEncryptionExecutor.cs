namespace Confluent.SchemaRegistry.Encryption.Aws
{
    public class AwsFieldEncryptionExecutor : FieldEncryptionExecutor
    {
        public AwsFieldEncryptionExecutor(string kmsKeyId, string accessKeyId, string secretAccessKey)
        {
            kekId = AwsKmsClient.Prefix + kmsKeyId;
            KmsClients.Add(kekId, new AwsKmsClient(kekId, accessKeyId, secretAccessKey));
        }
    }
}
namespace Confluent.SchemaRegistry.Encryption.Azure
{
    public class AzureFieldEncryptionExecutor : FieldEncryptionExecutor
    {
        public AzureFieldEncryptionExecutor(string kmsKeyId, string tenantId, string clientId, string clientSecret)
        {
            kekId = AzureKmsClient.Prefix + kmsKeyId;
            KmsClients.Add(kekId, new AzureKmsClient(kekId, tenantId, clientId, clientSecret));
        }
    }
}
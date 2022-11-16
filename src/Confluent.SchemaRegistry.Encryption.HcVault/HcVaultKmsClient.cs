using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using VaultSharp;
using VaultSharp.V1.AuthMethods;
using VaultSharp.V1.AuthMethods.Token;
using VaultSharp.V1.Commons;
using VaultSharp.V1.SecretsEngines.Transit;

namespace Confluent.SchemaRegistry.Encryption.HcVault
{
    public class HcVaultKmsClient : IKmsClient
    {
        public static readonly string Prefix = "hcvault://";

        private IVaultClient kmsClient;
        private string keyId;
        private string keyName;
        
        public string KekId { get; }
        public string TokenId { get; }
        
        public HcVaultKmsClient(string kekId, string tokenId)
        {
            KekId = kekId;
            TokenId = tokenId;
            
            if (!kekId.StartsWith(Prefix)) {
              throw new ArgumentException(string.Format($"key URI must start with {Prefix}"));
            }
            keyId = KekId.Substring(Prefix.Length);
            IAuthMethodInfo authMethod = new TokenAuthMethodInfo(tokenId);
            Uri uri = new Uri(keyId);
            keyName = uri.Segments[^1];
            
            var vaultClientSettings = new VaultClientSettings(uri.Scheme + "://" + uri.Authority, authMethod);
            kmsClient = new VaultClient(vaultClientSettings);
        }
        
        public async Task<byte[]> Encrypt(byte[] plaintext)
        {
            var encodedPlaintext = Convert.ToBase64String(plaintext);
            var encryptOptions = new EncryptRequestOptions
            {
                Base64EncodedPlainText = encodedPlaintext
            };

            Secret<EncryptionResponse> encryptionResponse = await kmsClient.V1.Secrets.Transit.EncryptAsync(keyName, encryptOptions);
            return Encoding.UTF8.GetBytes(encryptionResponse.Data.CipherText);
        }

        public async Task<byte[]> Decrypt(byte[] ciphertext)
        {
            var encodedCiphertext = Encoding.UTF8.GetString(ciphertext);
            var decryptOptions = new DecryptRequestOptions
            {
                CipherText = encodedCiphertext
            };

            Secret<DecryptionResponse> decryptionResponse = await kmsClient.V1.Secrets.Transit.DecryptAsync(keyName, decryptOptions);
            return Convert.FromBase64String(decryptionResponse.Data.Base64EncodedPlainText);
        }
    }
}
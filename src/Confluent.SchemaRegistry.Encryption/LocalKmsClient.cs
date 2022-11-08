using HkdfStandard;
using System.Security.Cryptography;
using System.Text;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalKmsClient : IKmsClient
    {
        public static readonly string Prefix = "local-kms://";
        
        public string Secret { get; }
        private Cryptor cryptor;
        private byte[] key;

        public LocalKmsClient(string secret)
        {
            Secret = secret;
            // TODO fix
            cryptor = new Cryptor(DekFormat.AES256_SIV);
            key = Hkdf.DeriveKey(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), cryptor.KeySize());
        }
        
        public byte[] Encrypt(byte[] plaintext)
        {
            return cryptor.Encrypt(key, plaintext);
        }

        public byte[] Decrypt(byte[] ciphertext)
        {
            return cryptor.Decrypt(key, ciphertext);
        }
    }
}
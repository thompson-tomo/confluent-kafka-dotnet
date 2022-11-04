using HkdfStandard;
using System.Security.Cryptography;
using System.Text;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalKmsClient : IKmsClient
    {
        public string Secret { get; }
        private byte[] key;
        private Cryptor cryptor;

        public LocalKmsClient(string secret)
        {
            Secret = secret;
            key = Hkdf.DeriveKey(HashAlgorithmName.SHA256, Encoding.UTF8.GetBytes(secret), 16);
            cryptor = new Cryptor(DekFormat.AES128_GCM);
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
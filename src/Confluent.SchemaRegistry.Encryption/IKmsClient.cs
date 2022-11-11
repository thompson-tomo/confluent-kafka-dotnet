using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public interface IKmsClient
    {
        Task<byte[]> Encrypt(byte[] plaintext);

        Task<byte[]> Decrypt(byte[] ciphertext);
    }
}
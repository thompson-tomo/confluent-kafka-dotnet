using System;
using System.IO;
using System.Security.Cryptography;
using Miscreant;

namespace Confluent.SchemaRegistry.Encryption
{
    public interface IKmsClient
    {
        byte[] Encrypt(byte[] plaintext);

        byte[] Decrypt(byte[] ciphertext);
    }
}
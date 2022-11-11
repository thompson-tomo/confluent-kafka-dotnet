using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Caching.Memory;

namespace Confluent.SchemaRegistry.Encryption
{
    public class LocalFieldEncryptionExecutor : FieldEncryptionExecutor
    {
        public LocalFieldEncryptionExecutor(String secret)
        {
            kekId = LocalKmsClient.Prefix;
            KmsClients.Add(kekId, new LocalKmsClient(secret));
        }
    }
}
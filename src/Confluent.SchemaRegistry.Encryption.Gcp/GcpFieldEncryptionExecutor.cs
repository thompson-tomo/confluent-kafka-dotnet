using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Caching.Memory;

namespace Confluent.SchemaRegistry.Encryption.Gcp
{
    public class GcpFieldEncryptionExecutor : FieldEncryptionExecutor
    {
        public GcpFieldEncryptionExecutor(string kmsKeyId)
        {
            kekId = GcpKmsClient.Prefix + kmsKeyId;
            KmsClients.Add(kekId, new GcpKmsClient(kekId));
        }
    }
}
﻿using System.Net.Security;

namespace KafkaClient.Connections
{
    /// <summary>
    /// BasicSslConfiguration in intended to use only for testing purposes.
    /// Accepts any valid server-side certificates, but doesn't require encrypted channel, or providing client-side certificate for the connection.
    /// For production use, it's strongly recommended to create Your own ISslConfiguration implementation, with stricter checks on the server-side certificate, forced channel encription, and using client-side certificate as well.
    /// </summary>
    public class SslConfiguration : ISslConfiguration
    {
        public SslConfiguration(RemoteCertificateValidationCallback remoteCertificateValidationCallback = null, 
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null, 
            EncryptionPolicy? encryptionPolicy = null)
        {
            RemoteCertificateValidationCallback = remoteCertificateValidationCallback ?? Defaults.RemoteCertificateValidationCallback;
            LocalCertificateSelectionCallback = localCertificateSelectionCallback ?? Defaults.LocalCertificateSelectionCallback;
            EncryptionPolicy = encryptionPolicy.GetValueOrDefault(Defaults.EncryptionPolicy);
        }

        public RemoteCertificateValidationCallback RemoteCertificateValidationCallback { get; }

        public LocalCertificateSelectionCallback LocalCertificateSelectionCallback { get; }

        public EncryptionPolicy? EncryptionPolicy { get; }

        public static class Defaults
        {
            public static readonly RemoteCertificateValidationCallback RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => sslPolicyErrors == SslPolicyErrors.None;

            public static readonly LocalCertificateSelectionCallback LocalCertificateSelectionCallback = (sender, targetHost, localCertificates, remoteCertificate, acceptableIssuers) => null;

            public const EncryptionPolicy EncryptionPolicy = System.Net.Security.EncryptionPolicy.RequireEncryption;
        }
    }
}

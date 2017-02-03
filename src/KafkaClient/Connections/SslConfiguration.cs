using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace KafkaClient.Connections
{
    /// <summary>
    /// BasicSslConfiguration in intended to use only for testing purposes.
    /// Accepts any valid server-side certificates, but doesn't require encrypted channel, or providing client-side certificate for the connection.
    /// For production use, it's strongly recommended to create Your own ISslConfiguration implementation, with stricter checks on the server-side certificate, forced channel encription, and using client-side certificate as well.
    /// </summary>
    public class SslConfiguration : ISslConfiguration
    {
        public SslConfiguration(RemoteCertificateValidationCallback remoteValidation = null, 
            LocalCertificateSelectionCallback localSelection = null,
            X509CertificateCollection localCollection = null,
            EncryptionPolicy? encryptionPolicy = null,
            SslProtocols? enabledProtocols = null,
            bool checkRevocation = false)
        {
            RemoteCertificateValidationCallback = remoteValidation ?? Defaults.RemoteCertificateValidationCallback;
            LocalCertificateSelectionCallback = localSelection ?? Defaults.LocalCertificateSelectionCallback;
            LocalCertificates = localCollection ?? new X509CertificateCollection();
            EncryptionPolicy = encryptionPolicy.GetValueOrDefault(Defaults.EncryptionPolicy);
            EnabledProtocols = enabledProtocols.GetValueOrDefault(Defaults.EnabledProtocols);
            CheckCertificateRevocation = checkRevocation;
        }

        public RemoteCertificateValidationCallback RemoteCertificateValidationCallback { get; }

        public LocalCertificateSelectionCallback LocalCertificateSelectionCallback { get; }

        public X509CertificateCollection LocalCertificates { get; }

        public EncryptionPolicy EncryptionPolicy { get; }

        public SslProtocols EnabledProtocols { get; }

        public bool CheckCertificateRevocation { get; }

        public static class Defaults
        {
            public static readonly RemoteCertificateValidationCallback RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => sslPolicyErrors == SslPolicyErrors.None;

            public static readonly LocalCertificateSelectionCallback LocalCertificateSelectionCallback =
                (sender, targetHost, localCertificates, remoteCertificate, acceptableIssuers) => {
                    if (acceptableIssuers?.Length > 0 && localCertificates?.Count > 0) {
                        foreach (var certificate in localCertificates) {
                            if (acceptableIssuers.Any(acceptable => certificate.Issuer == acceptable)) return certificate;
                        }
                    }
                    return localCertificates?.Count > 0 ? localCertificates[0] : null;
                };

            public const EncryptionPolicy EncryptionPolicy = System.Net.Security.EncryptionPolicy.RequireEncryption;
            public const SslProtocols EnabledProtocols = SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12;
        }
    }
}

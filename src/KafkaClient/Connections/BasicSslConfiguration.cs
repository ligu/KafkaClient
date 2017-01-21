using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace KafkaClient.Connections
{
    /// <summary>
    /// BasicSslConfiguration in intended to use only for testing purposes.
    /// Accepts any valid server-side certificates, but doesn't require encrypted channel, or providing client-side certificate for the connection.
    /// For production use, it's strongly recommended to create Your own ISslConfiguration implementation, with stricter checks on the server-side certificate, forced channel encription, and using client-side certificate as well.
    /// </summary>
    public class BasicSslConfiguration : ISslConfiguration
    {
        public RemoteCertificateValidationCallback RemoteCertificateValidationCallback
        {
            get
            {
                return delegate (object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
                {
                    if (sslPolicyErrors == SslPolicyErrors.None)
                        return true;

                    return false;
                };
            }
        }

        public LocalCertificateSelectionCallback LocalCertificateSelectionCallback
        {
            get
            {
                return delegate (object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers)
                {
                    return null;
                };
            }
        }

        public EncryptionPolicy? EncryptionPolicy
        {
            get
            {
                return System.Net.Security.EncryptionPolicy.AllowNoEncryption;
            }
        }

    }
}

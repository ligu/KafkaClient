using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace KafkaClient.Connections
{
    public interface ISslConfiguration
    {
        RemoteCertificateValidationCallback RemoteCertificateValidationCallback { get; }

        LocalCertificateSelectionCallback LocalCertificateSelectionCallback { get; }

        X509CertificateCollection LocalCertificates { get; }

        EncryptionPolicy EncryptionPolicy { get; }

        SslProtocols EnabledProtocols { get; }

        bool CheckCertificateRevocation { get; }
    }
}

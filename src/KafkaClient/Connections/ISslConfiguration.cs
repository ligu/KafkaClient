using System.Net.Security;

namespace KafkaClient.Connections
{
    public interface ISslConfiguration
    {
        RemoteCertificateValidationCallback RemoteCertificateValidationCallback { get; }

        LocalCertificateSelectionCallback LocalCertificateSelectionCallback { get; }

        EncryptionPolicy? EncryptionPolicy { get; }
    }
}

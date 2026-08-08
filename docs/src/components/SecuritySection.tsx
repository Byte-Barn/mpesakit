// components/SecuritySection.tsx

import styles from './MpesaKit.module.css';
interface SecurityItem {
  icon: string;
  title: string;
  description: string;
}

interface SecuritySectionProps {
  title: string;
  description: string;
  securityFeatures: SecurityItem[];
}

export const SecuritySection: React.FC<SecuritySectionProps> = ({
  title,
  description,
  securityFeatures
}) => {
  return (
    <section className={`${styles.security} ${styles.fadeInUp}`} id="security">
      <div className={styles.securityContainer}>
        <div className={styles.securityContent}>
          <h2>{title}</h2>
          <p style={{ color: 'var(--text-secondary)', fontSize: '1.1rem', marginBottom: '2rem' }}>
            {description}
          </p>

          <div className={styles.securityFeatures}>
            {securityFeatures.map((item, index) => (
              <div key={index} className={styles.securityItem}>
                <div className={styles.securityIcon}>{item.icon}</div>
                <div>
                  <h4>{item.title}</h4>
                  <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
                    {item.description}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <SecurityVisual />
      </div>
    </section>
  );
};

const SecurityVisual: React.FC = () => {
  const securityLayers = [
    {
      title: 'Network Security',
      description: 'TLS encryption, IP filtering, HTTPS enforcement',
      color: 'var(--accent-blue)'
    },
    {
      title: 'Authentication',
      description: 'OAuth2, token validation, credential management',
      color: 'var(--accent-purple)'
    },
    {
      title: 'Data Validation',
      description: 'Schema validation, input sanitization, type checking',
      color: 'var(--mpesa-green)'
    }
  ];

  return (
    <div className={styles.securityVisual}>
      <div style={{
        background: 'var(--gradient-dark)',
        color: 'var(--terminal-text)',
        borderRadius: '20px',
        padding: '2rem',
        border: '1px solid rgba(59, 130, 246, 0.2)'
      }}>
        <h3 style={{ color: 'var(--accent-blue)', marginBottom: '1.5rem', textAlign: 'center' }}>
          Security Layers
        </h3>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          {securityLayers.map((layer, index) => (
            <div
              key={index}
              style={{
                background: `${layer.color.replace('var(--', '').replace(')', '')}10`,
                padding: '1rem',
                borderRadius: '10px',
                borderLeft: `3px solid ${layer.color}`
              }}
            >
              <strong>{layer.title}</strong><br />
              <small style={{ color: 'var(--terminal-text-secondary)' }}>{layer.description}</small>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

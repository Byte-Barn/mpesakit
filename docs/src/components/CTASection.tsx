// components/CTASection.tsx
import styles from './MpesaKit.module.css';

interface CTASectionProps {
  title: string;
  description: string;
  primaryCTA: {
    text: string;
    href: string;
  };
  secondaryCTA: {
    text: string;
    href: string;
  };
  installCommand?: string;
}

export const CTASection: React.FC<CTASectionProps> = ({
  title,
  description,
  primaryCTA,
  secondaryCTA,
  installCommand = 'pip install mpesakit'
}) => {
  return (
    <section className={`${styles.ctaSection} ${styles.fadeInUp}`}>
      <div className={styles.ctaContainer}>
        <h2>{title}</h2>
        <p>{description}</p>

        <div className={styles.ctaButtons}>
          <a href={primaryCTA.href} className={`${styles.btn} ${styles.btnPrimary}`}>
            {primaryCTA.text}
          </a>
          <a href={secondaryCTA.href} className={`${styles.btn} ${styles.btnSecondary}`}>
            {secondaryCTA.text}
          </a>
        </div>

        {installCommand && (
          <div style={{
            marginTop: '3rem',
            padding: '2rem',
            background: 'var(--card-bg)',
            borderRadius: '15px',
            border: '1px solid var(--card-border)'
          }}>
            <h3 style={{ marginBottom: '1rem' }}>Installation</h3>
            <div style={{
              background: 'var(--gradient-dark)',
              padding: '1rem',
              borderRadius: '10px',
              fontFamily: 'monospace',
              textAlign: 'left'
            }}>
              <span style={{ color: 'var(--terminal-text-secondary)' }}>$</span>{' '}
              <span style={{ color: 'var(--mpesa-green)' }}>{installCommand}</span>
            </div>
          </div>
        )}
      </div>
    </section>
  );
};

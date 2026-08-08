// components/CodeDemo.tsx
import styles from './MpesaKit.module.css';

export const CodeDemo: React.FC = () => {
  const codeLines = [
    { content: 'import os', type: 'code' },
    { content: 'from dotenv import load_dotenv', type: 'import' },
    { content: 'from mpesakit import MpesaClient', type: 'import' },
    { content: 'from mpesakit.mpesa_express import TransactionType', type: 'import' },
    { content: '', type: 'empty' },
    { content: '# load enviroment variables', type: 'comment' },
    { content: 'load_dotenv()', type: 'code' },
    { content: '', type: 'empty' },
    { content: 'client = MpesaClient(', type: 'code' },
    { content: '    consumer_key=os.getenv("MPESA_CONSUMER_KEY"),', type: 'code' },
    { content: '    consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),', type: 'code' },
    { content: '    environment="sandbox",', type: 'code' },
    { content: ')', type: 'code' },
    { content: '', type: 'empty' },
    { content: '# Send STK Push — Coffee Purchase', type: 'comment' },
    { content: 'response = client.stk_push(', type: 'code' },
    { content: '    business_short_code=int(os.getenv("MPESA_SHORTCODE")),', type: 'code' },
    { content: '    passkey=os.getenv("MPESA_PASSKEY"),', type: 'code' },
    { content: '    transaction_type=TransactionType.CUSTOMER_PAYBILL_ONLINE,', type: 'code' },
    { content: '    amount=250,', type: 'code' },
    { content: '    party_a=os.getenv("MPESA_PHONE_NUMBER"),', type: 'code' },
    { content: '    party_b=os.getenv("MPESA_SHORTCODE"),', type: 'code' },
    { content: '    phone_number=os.getenv("MPESA_PHONE_NUMBER"),', type: 'code' },
    { content: '    callback_url="https://example.com/callback",', type: 'code' },
    { content: '    account_reference="NairobiCafe-Order123",', type: 'code' },
    { content: '    transaction_desc="Coffee — Nairobi Cafe (Order #123)",', type: 'code' },
    { content: ')', type: 'code' }
  ];

  return (
    <div className={styles.codeDemo}>
      <div className={styles.codeHeader}>
        <div className={styles.codeDots}>
          <div className={`${styles.dot} ${styles.red}`}></div>
          <div className={`${styles.dot} ${styles.yellow}`}></div>
          <div className={`${styles.dot} ${styles.green}`}></div>
        </div>
        <span style={{ marginLeft: '1rem', color: 'var(--terminal-text-secondary)' }}>quick_start.py</span>
      </div>
      <div className={styles.codeContent}>
        {codeLines.map((line, index) => (
          <div key={index} className={styles.codeLine}>
            {line.type === 'import' && (
              <>
                <span className={styles.keyword}>from</span>{' '}
                <span className={styles.string}>{line.content.split(' ')[1]}</span>{' '}
                <span className={styles.keyword}>import</span>{' '}
                <span className={styles.function}>{line.content.split(' ')[3]}</span>
              </>
            )}
            {line.type === 'comment' && (
              <span className={styles.comment}>{line.content}</span>
            )}
            {line.type === 'code' && (
              <span style={{ whiteSpace: 'pre' }}>{line.content}</span>
            )}
            {line.type === 'empty' && <br />}
          </div>
        ))}
      </div>
    </div>
  );
};

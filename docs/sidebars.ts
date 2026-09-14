import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: 'doc',
      id: 'intro',
      label: 'Introduction',
    },
    {
      type: 'doc',
      id: 'production',
      label: 'Production Requirements',
    },
    {
      type: 'doc',
      id: 'getting-credentials',
      label: 'Getting Credentials',
    },
    {
      type: 'doc',
      id: 'environment',
      label: 'Environment Configuration',
    },
    {
      type: 'doc',
      id: 'installation',
      label: 'Installation',
    },
    {
      type: 'category',
      label: 'APIReference',
      items: [
        {
          type: 'doc',
          id: 'auth',
          label: 'Authentication & Token Management',
        },
        {
          type: 'category',
          label: 'Mpesa Express',
          collapsed: true,
          items: [
            {
              type: 'doc',
              id: 'mpesa-express/stk-push',
              label: 'STK Push',
            },
            {
              type: 'doc',
              id: 'mpesa-express/stk-query',
              label: 'STK Query',
            },
          ],
        },
        {
          type: 'doc',
          id: 'dynamic-qr',
          label: 'Dynamic QR Code',
        },
        {
          type: 'doc',
          id: 'c2b',
          label: 'Customer to Business (C2B)',
        },
        {
          type: 'doc',
          id: 'b2c',
          label: 'Business to Customer (B2C)',
        },
        {
          type: 'doc',
          id: 'transaction-status',
          label: 'Transaction Status',
        },
        {
          type: 'doc',
          id: 'account-balance',
          label: 'Account Balance',
        },
        {
          type: 'doc',
          id: 'reversal',
          label: 'Reversal',
        },
        {
          type: 'doc',
          id: 'tax-remittance',
          label: 'Tax Remittance',
        },
        {
          type: 'doc',
          id: 'business-paybill',
          label: 'Business Paybill',
        },
        {
          type: 'doc',
          id: 'business-buygoods',
          label: 'Business Buygoods',
        },
        {
          type: 'doc',
          id: 'b2c-account-top-up',
          label: 'B2C Account Top Up',
        },
        {
          type: 'doc',
          id: 'swap',
          label: 'SIM Swap Detection',
        },
      ],
    },
    {
      type: 'doc',
      id: 'webhooks-best-practices',
      label: 'Webhooks Best Practices',
    },
  ],
};

export default sidebars;
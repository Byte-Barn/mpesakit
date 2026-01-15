import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'MpesaKit',
  tagline: 'Mpesa SDK  for integrating M-Pesa APIs in Python applications',
  // Improve compatibility with Docusaurus v4
  future: {
    v4: true,
  },

  // Production URL (GitHub Pages URL)
  url: 'https://rafaeljohn9.github.io', // Your GitHub Pages URL
  baseUrl: '/', // Project repo name as base path

  // GitHub Pages deployment settings
  organizationName: 'rafaeljohn9', // GitHub username
  projectName: 'mpesakit', // Repository name
  deploymentBranch: 'master', // Default for GitHub Pages

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Enable auto-sidebar if you don't want to manually define categories
          routeBasePath: '/', // Serve docs at site root (optional)
          sidebarCollapsible: true, // Enable collapsible categories
          sidebarCollapsed: false, // Keep categories expanded by default
        },
        // blog: {
        //   showReadingTime: true,
        //   feedOptions: {
        //     type: ['rss', 'atom'],
        //     xslt: true,
        //   },
        //   editUrl:
        //     'https://github.com/rafaeljohn9/mpesakit/tree/main/blog/',
        //   onInlineTags: 'warn',
        //   onInlineAuthors: 'warn',
        //   onUntruncatedBlogPosts: 'warn',
        // },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    Favicon: 'img/favicon.ico',
    Image: 'img/favicon.svg',
    docs: {
      sidebar: {
        hideable: false, // This is the default, shows the sidebar
        autoCollapseCategories: false, // This keeps categories expanded unless manually collapsed
      },
    },
    colorMode: {
      defaultMode: 'dark', // This sets dark mode as the default
      disableSwitch: false, // Keep the switch visible so users can toggle
      respectPrefersColorScheme: false, // Ignore system preference and use defaultMode
    },
    navbar: {
      title: 'MpesaKit',
      logo: {
        alt: 'MpesaKit Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Docs',
        },
        // { to: '/blog', label: 'Blog', position: 'left' }, // Uncomment if blogs have been added
        {
          href: 'https://github.com/rafaeljohn9/mpesakit',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://discord.gg/hNxTew523E',
          label: 'Discord',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Introduction',
              to: '/',
            },
            {
              label: 'Installation',
              to: '/installation',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub Issues',
              href: 'https://github.com/rafaeljohn9/mpesakit/issues',
            },
            // {
            //   label: 'Discord',
            //   href: 'https://discord.gg/hNxTew523E', // Optional: create your own
            // },
            // {
            //   label: 'Stack Overflow',
            //   href: 'https://stackoverflow.com/questions/tagged/docusaurus',
            // },
          ],
        },
        {
          title: 'More',
          items: [
            // {
            //   label: 'Blog',
            //   to: '/blog',
            // },
            {
              label: 'GitHub',
              href: 'https://github.com/rafaeljohn9/mpesakit',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} mpesakit.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,

  // Optional: Enable trailing slash consistency
  trailingSlash: false, // GitHub Pages usually prefers no trailing slash
};

export default config;
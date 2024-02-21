// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
import { Highlight, themes } from "prism-react-renderer"

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Smart Data Lake Builder',
  tagline: 'A smart Automation Tool for building modern Data Lakes and Data Pipelines',
  url: 'https://www.smartdatalake.ch',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'smart-data-lake', // Usually your GitHub org/user name.
  projectName: 'smart-data-lake', // Usually your repo name.

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/smart-data-lake/smart-data-lake/tree/documentation',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/smart-data-lake/smart-data-lake/tree/documentation',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
      /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
      ({
        navbar: {
          title: 'Smart Data Lake',
          logo: {
            alt: 'Smart Data Lake Logo',
            src: 'img/sdl_logo.png',
          },
          items: [
            {
              to: 'docs/',
              activeBasePath: 'docs',
              label: 'Docs',
              position: 'left',
            },
            {to: 'blog', label: 'Blog', position: 'left'},
            {to: 'json-schema-viewer', label: 'SchemaViewer', position: 'left'},
            {href: 'https://ui-demo.smartdatalake.ch/', label: 'UI-Demo', position: 'left'},
            {
              href: 'https://github.com/smart-data-lake/smart-data-lake',
              label: 'GitHub',
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
                  label: 'Getting started',
                  to: 'docs/',
                },
              ],
            },
            {
              title: 'Community',
              items: [
                {
                  label: 'GitHub',
                  href: 'https://github.com/smart-data-lake/smart-data-lake',
                },
                {
                  label: 'GitHub Issues',
                  href: 'https://github.com/smart-data-lake/smart-data-lake/issues',
                },
              ],
            },
            {
              title: 'More',
              items: [
                // {
                //   label: 'Blog',
                //   to: 'blog',
                // },
                {
                  label: 'ELCA',
                  href: 'https://www.elca.ch'
                }
              ],
            },
          ],
          copyright: `Copyright © ${new Date().getFullYear()} Smart Data Lake, Built with Docusaurus.`,
        },
        prism: {
            theme: themes.vsLight,
            darkTheme: themes.dracula
            //additionalLanguages: ['scala'],
      },
      algolia: {
        // The application ID provided by Algolia
        appId: 'UOM3ZOMCU0',

        // Public API key: it is safe to commit it
        apiKey: '83bd98a629daf2b2aa34487d5e061c06',

        indexName: 'smartdatalake',

        // Optional: see doc section below
        contextualSearch: true,

        // Optional: path for search page that enabled by default (`false` to disable it)
        searchPagePath: 'search',
      },
      zoom: {
        selector: '.markdown .nozoom',
        background: {
          light: 'rgb(255, 255, 255)',
          dark: 'rgb(50, 50, 50)'
        },
        config: {
          // options you can specify via https://github.com/francoischalifour/medium-zoom#usage
        }
      }
    })
};

module.exports = config;

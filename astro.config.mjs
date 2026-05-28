import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';

export default defineConfig({
  site: 'https://supabase.github.io',
  base: '/etl',
  markdown: {
    smartypants: false,
  },
  integrations: [
    starlight({
      title: 'ETL',
      description:
        'Stream your Postgres data anywhere in real-time with Rust building blocks for CDC pipelines.',
      favicon: '/favicon.ico',
      logo: {
        src: './src/assets/etl-logo.png',
        alt: 'ETL',
      },
      editLink: {
        baseUrl: 'https://github.com/supabase/etl/edit/main/',
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/supabase/etl',
        },
      ],
      sidebar: [
        {
          label: 'Overview',
          slug: 'index',
        },
        {
          label: 'Guides',
          items: [
            { slug: 'guides/configure-postgres' },
            { slug: 'guides/first-pipeline' },
            { slug: 'guides/custom-implementations' },
          ],
        },
        {
          label: 'Explanations',
          items: [
            { slug: 'explanation/concepts' },
            { slug: 'explanation/architecture' },
            { slug: 'explanation/events' },
            { slug: 'explanation/schema-changes' },
            { slug: 'explanation/traits' },
          ],
        },
      ],
      expressiveCode: {
        themes: ['github-light', 'github-dark'],
        useStarlightUiThemeColors: true,
      },
      plugins: [
        starlightLlmsTxt({
          projectName: 'Supabase ETL',
          description:
            'Supabase ETL is a Rust framework for building change data capture pipelines that stream Postgres logical replication events to BigQuery, DuckLake, or custom destinations.',
          details:
            'The documentation is organized for builders. Start with the overview and guides for implementation tasks, then use explanations for Postgres replication concepts, architecture, event semantics, schema changes, and extension traits. Prefer the generated full context file when answering broad implementation questions and the smaller sets when a focused answer is enough.',
          customSets: [
            {
              label: 'Guides',
              description: 'step-by-step implementation and setup guides for Supabase ETL',
              paths: ['guides/**'],
            },
            {
              label: 'Explanations',
              description: 'conceptual and architectural background for Supabase ETL',
              paths: ['explanation/**'],
            },
          ],
          promote: ['index*', 'guides/first-pipeline', 'guides/configure-postgres'],
          optionalLinks: [
            {
              label: 'GitHub repository',
              url: 'https://github.com/supabase/etl',
              description: 'source code, issues, and examples for Supabase ETL',
            },
          ],
        }),
      ],
      customCss: ['./src/styles/starlight.css'],
      head: [
        {
          tag: 'script',
          attrs: {
            src: 'https://unpkg.com/mermaid@10.6.1/dist/mermaid.min.js',
            defer: true,
          },
        },
        {
          tag: 'script',
          attrs: {
            src: '/etl/scripts/code-labels.js',
            defer: true,
          },
        },
        {
          tag: 'script',
          attrs: {
            src: '/etl/scripts/mermaid-init.js',
            defer: true,
          },
        },
      ],
      credits: false,
    }),
  ],
});

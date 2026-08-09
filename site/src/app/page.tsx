import { AgentActions } from '@/components/agent-actions';
import { HomeNav } from '@/components/home-nav';
import { ReplicationFlow } from '@/components/replication-flow';
import { absoluteUrl, siteConfig, withBasePath } from '@/lib/site';
import { ArrowRight, CodeXml } from 'lucide-react';
import type { Metadata } from 'next';
import Link from 'next/link';

const homeDescription = siteConfig.description;

export const metadata: Metadata = {
  title: { absolute: siteConfig.title },
  description: homeDescription,
  keywords: [
    'Supabase ETL',
    'Postgres replication',
    'PostgreSQL logical replication',
    'Postgres CDC',
    'Rust data pipeline',
  ],
  alternates: {
    canonical: absoluteUrl('/'),
    types: { 'text/markdown': absoluteUrl('/index.md') },
  },
  openGraph: {
    type: 'website',
    url: absoluteUrl('/'),
    siteName: siteConfig.name,
    title: siteConfig.title,
    description: homeDescription,
    images: [
      {
        url: absoluteUrl('/opengraph-image.png'),
        width: 1200,
        height: 630,
        alt: siteConfig.title,
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: siteConfig.title,
    description: homeDescription,
    images: [absoluteUrl('/opengraph-image.png')],
  },
};

const structuredData = [
  {
    '@context': 'https://schema.org',
    '@type': 'Organization',
    '@id': 'https://supabase.com/#organization',
    name: 'Supabase',
    url: 'https://supabase.com',
  },
  {
    '@context': 'https://schema.org',
    '@type': 'WebSite',
    '@id': `${absoluteUrl('/')}#website`,
    name: 'Supabase ETL Documentation',
    url: absoluteUrl('/'),
    publisher: { '@id': 'https://supabase.com/#organization' },
  },
  {
    '@context': 'https://schema.org',
    '@type': 'SoftwareSourceCode',
    name: 'Supabase ETL',
    description: homeDescription,
    url: absoluteUrl('/'),
    codeRepository: siteConfig.repository,
    programmingLanguage: 'Rust',
    runtimePlatform: 'PostgreSQL',
    author: { '@id': 'https://supabase.com/#organization' },
    license: 'https://www.apache.org/licenses/LICENSE-2.0',
  },
];

export default function HomePage() {
  return (
    <>
      <main className="etl-landing">
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify(structuredData).replaceAll('<', '\\u003c'),
          }}
        />
        <HomeNav />
        <ReplicationFlow />

        <section className="etl-landing-content" aria-labelledby="etl-home-title">
          <div className="etl-landing-grid">
            <div className="etl-landing-primary">
              <h1 id="etl-home-title">
                <span>Postgres replication,</span>
                <span>in Rust.</span>
              </h1>
            </div>

            <div className="etl-landing-summary">
              <p>{homeDescription}</p>
            </div>

            <div className="etl-landing-actions">
              <Link className="etl-primary-action" href="/guides/first-pipeline/">
                Build your first pipeline <ArrowRight aria-hidden="true" strokeWidth={1.5} />
              </Link>
              <a
                className="etl-secondary-action"
                href={siteConfig.repository}
                target="_blank"
                rel="noreferrer"
              >
                <CodeXml aria-hidden="true" strokeWidth={1.5} /> View on GitHub
              </a>
            </div>

            <AgentActions markdownUrl="/index.md" pagePath="/" />
          </div>
        </section>
      </main>

      <footer className="etl-home-footer">
        <div className="etl-home-footer-inner">
          <p>Copyright © {new Date().getFullYear()} Supabase Inc.</p>
          <nav aria-label="Footer navigation">
            <Link href="/guides/first-pipeline/">Docs</Link>
            <a href={siteConfig.supabaseDocs}>Supabase Pipelines</a>
            <a href={siteConfig.repository}>GitHub</a>
            <a href={withBasePath('/llms.txt')}>llms.txt</a>
          </nav>
        </div>
      </footer>
    </>
  );
}

import { AgentActions } from '@/components/agent-actions';
import { getMDXComponents } from '@/components/mdx';
import { getPageImageUrl, getPageMarkdownUrl, source } from '@/lib/source';
import { absoluteUrl, siteConfig } from '@/lib/site';
import { createRelativeLink } from 'fumadocs-ui/mdx';
import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
} from 'fumadocs-ui/layouts/docs/page';
import type { Metadata } from 'next';
import { notFound } from 'next/navigation';

type PageRouteProps = {
  params: Promise<{ slug: string[] }>;
};

export default async function Page({ params }: PageRouteProps) {
  const { slug } = await params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const MDX = page.data.body;
  const markdownUrl = getPageMarkdownUrl(page).url;
  const jsonLd = buildStructuredData(page);

  return (
    <DocsPage toc={page.data.toc} full={page.data.full}>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd).replaceAll('<', '\\u003c') }}
      />
      <header className="etl-page-heading">
        <div className="etl-eyebrow">Supabase ETL documentation</div>
        <DocsTitle>{page.data.title}</DocsTitle>
        <DocsDescription>{page.data.description}</DocsDescription>
      </header>
      <div className="etl-page-actions">
        <AgentActions markdownUrl={markdownUrl} pagePath={page.url} />
      </div>
      <DocsBody>
        <MDX components={getMDXComponents({ a: createRelativeLink(source, page) })} />
      </DocsBody>
    </DocsPage>
  );
}

export function generateStaticParams() {
  return source
    .getPages()
    .filter((page) => page.slugs.length > 0)
    .map((page) => ({ slug: page.slugs }));
}

export async function generateMetadata({ params }: PageRouteProps): Promise<Metadata> {
  const { slug } = await params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const canonical = absoluteUrl(page.url);
  const image = absoluteUrl(getPageImageUrl(page).url);

  return {
    title: page.data.title,
    description: page.data.description,
    keywords: [page.data.title, 'Supabase ETL', 'Postgres CDC', 'Rust data pipeline'],
    alternates: {
      canonical,
      types: { 'text/markdown': absoluteUrl(getPageMarkdownUrl(page).url) },
    },
    openGraph: {
      type: 'article',
      url: canonical,
      siteName: siteConfig.name,
      title: page.data.title,
      description: page.data.description,
      images: [{ url: image, width: 1200, height: 630, alt: page.data.title }],
    },
    twitter: {
      card: 'summary_large_image',
      title: page.data.title,
      description: page.data.description,
      images: [image],
    },
  };
}

function buildStructuredData(page: (typeof source)['$inferPage']) {
  const canonical = absoluteUrl(page.url);

  return {
    '@context': 'https://schema.org',
    '@type': 'TechArticle',
    headline: page.data.title,
    description: page.data.description,
    url: canonical,
    mainEntityOfPage: canonical,
    author: { '@type': 'Organization', name: 'Supabase', url: 'https://supabase.com' },
    publisher: {
      '@type': 'Organization',
      name: 'Supabase',
      url: 'https://supabase.com',
    },
    isPartOf: {
      '@type': 'CreativeWorkSeries',
      name: 'Supabase ETL Documentation',
      url: absoluteUrl('/'),
    },
    about: [
      { '@type': 'SoftwareSourceCode', name: 'Supabase ETL', codeRepository: siteConfig.repository },
      { '@type': 'Thing', name: 'PostgreSQL logical replication' },
      { '@type': 'Thing', name: 'Change data capture' },
    ],
  };
}

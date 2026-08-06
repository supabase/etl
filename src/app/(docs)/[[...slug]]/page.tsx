import { HeroBrand } from '@/components/brand';
import { getMDXComponents } from '@/components/mdx';
import { getPageImageUrl, getPageMarkdownUrl, source } from '@/lib/source';
import { absoluteUrl, siteConfig, withBasePath } from '@/lib/site';
import { createRelativeLink } from 'fumadocs-ui/mdx';
import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
  MarkdownCopyButton,
  ViewOptionsPopover,
} from 'fumadocs-ui/layouts/docs/page';
import { ArrowRight, CodeXml } from 'lucide-react';
import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';

type PageRouteProps = {
  params: Promise<{ slug?: string[] }>;
};

export default async function Page({ params }: PageRouteProps) {
  const { slug } = await params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const MDX = page.data.body;
  const isHome = page.slugs.length === 0;
  const markdownUrl = withBasePath(getPageMarkdownUrl(page).url);
  const githubUrl = `${siteConfig.repository}/blob/main/docs/src/content/docs/${page.path}`;
  const jsonLd = buildStructuredData(page);

  return (
    <DocsPage
      toc={page.data.toc}
      full={page.data.full}
      breadcrumb={{ enabled: !isHome }}
      className={isHome ? 'etl-home-page' : undefined}
    >
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd).replaceAll('<', '\\u003c') }}
      />
      {isHome ? (
        <header className="etl-hero">
          <HeroBrand />
          <DocsTitle className="etl-hero-title">Postgres replication for Rust.</DocsTitle>
          <DocsDescription className="etl-hero-description">{page.data.description}</DocsDescription>
          <div className="etl-hero-actions">
            <Link className="etl-primary-action" href="/guides/first-pipeline/">
              Build your first pipeline <ArrowRight size={16} />
            </Link>
            <a className="etl-secondary-action" href={siteConfig.repository}>
              <CodeXml size={16} /> View on GitHub
            </a>
          </div>
          <div className="etl-development-note">Under active development</div>
        </header>
      ) : (
        <header className="etl-page-heading">
          <div className="etl-eyebrow">Supabase ETL documentation</div>
          <DocsTitle>{page.data.title}</DocsTitle>
          <DocsDescription>{page.data.description}</DocsDescription>
        </header>
      )}
      <div className="etl-page-actions">
        <MarkdownCopyButton markdownUrl={markdownUrl} />
        <ViewOptionsPopover markdownUrl={markdownUrl} githubUrl={githubUrl} />
      </div>
      <DocsBody className={isHome ? 'etl-home-content' : undefined}>
        <MDX components={getMDXComponents({ a: createRelativeLink(source, page) })} />
      </DocsBody>
    </DocsPage>
  );
}

export function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata({ params }: PageRouteProps): Promise<Metadata> {
  const { slug } = await params;
  const page = source.getPage(slug);
  if (!page) notFound();

  const canonical = absoluteUrl(page.url);
  const image = absoluteUrl(getPageImageUrl(page).url);
  const isHome = page.slugs.length === 0;
  const title = isHome ? siteConfig.title : page.data.title;

  return {
    title: isHome ? { absolute: siteConfig.title } : page.data.title,
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
      title,
      description: page.data.description,
      images: [{ url: image, width: 1200, height: 630, alt: title }],
    },
    twitter: {
      card: 'summary_large_image',
      title,
      description: page.data.description,
      images: [image],
    },
  };
}

function buildStructuredData(page: (typeof source)['$inferPage']) {
  const canonical = absoluteUrl(page.url);
  const isHome = page.slugs.length === 0;
  const article = {
    '@context': 'https://schema.org',
    '@type': 'TechArticle',
    headline: isHome ? siteConfig.title : page.data.title,
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

  if (!isHome) return article;

  return [
    article,
    {
      '@context': 'https://schema.org',
      '@type': 'SoftwareSourceCode',
      name: 'Supabase ETL',
      description: siteConfig.description,
      url: absoluteUrl('/'),
      codeRepository: siteConfig.repository,
      programmingLanguage: 'Rust',
      runtimePlatform: 'PostgreSQL',
      author: { '@type': 'Organization', name: 'Supabase', url: 'https://supabase.com' },
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
    },
  ];
}

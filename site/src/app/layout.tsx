import { Provider } from '@/components/provider';
import { absoluteUrl, siteConfig, withBasePath } from '@/lib/site';
import type { Metadata, Viewport } from 'next';
import type { ReactNode } from 'react';
import './global.css';

export const metadata: Metadata = {
  metadataBase: new URL(`${siteConfig.origin}${siteConfig.basePath}/`),
  title: {
    default: siteConfig.title,
    template: `%s | ${siteConfig.name}`,
  },
  description: siteConfig.description,
  applicationName: siteConfig.name,
  category: 'technology',
  keywords: [
    'Supabase',
    'ETL',
    'Postgres replication',
    'PostgreSQL logical replication',
    'change data capture',
    'CDC',
    'Rust',
    'data pipelines',
    'BigQuery',
    'DuckLake',
  ],
  authors: [{ name: 'Supabase', url: 'https://supabase.com' }],
  creator: 'Supabase',
  publisher: 'Supabase',
  icons: {
    icon: [
      {
        url: withBasePath('/assets/etl-favicon.svg'),
        type: 'image/svg+xml',
        sizes: 'any',
      },
    ],
    shortcut: withBasePath('/assets/etl-favicon.svg'),
  },
  alternates: {
    canonical: absoluteUrl('/'),
  },
  openGraph: {
    type: 'website',
    locale: 'en_US',
    url: absoluteUrl('/'),
    siteName: siteConfig.name,
    title: siteConfig.title,
    description: siteConfig.description,
    images: [{ url: absoluteUrl('/opengraph-image.png'), width: 1200, height: 630 }],
  },
  twitter: {
    card: 'summary_large_image',
    title: siteConfig.title,
    description: siteConfig.description,
    images: [absoluteUrl('/opengraph-image.png')],
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      'max-image-preview': 'large',
      'max-snippet': -1,
      'max-video-preview': -1,
    },
  },
};

export const viewport: Viewport = {
  colorScheme: 'dark light',
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#fbfdfc' },
    { media: '(prefers-color-scheme: dark)', color: '#0c1210' },
  ],
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <link
          rel="alternate"
          type="text/plain"
          href={withBasePath('/llms.txt')}
          title="Supabase ETL documentation index for AI agents"
        />
        <link
          rel="alternate"
          type="text/plain"
          href={withBasePath('/llms-full.txt')}
          title="Complete Supabase ETL documentation for AI agents"
        />
        <link
          rel="alternate"
          type="application/json"
          href={withBasePath('/agents.json')}
          title="Supabase ETL agent manifest"
        />
      </head>
      <body>
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}

'use client';

import { Brand } from '@/components/brand';
import { siteConfig } from '@/lib/site';
import {
  FullSearchTrigger,
  SearchTrigger,
} from 'fumadocs-ui/layouts/shared/slots/search-trigger';
import { ThemeSwitch } from 'fumadocs-ui/layouts/shared/slots/theme-switch';
import Link from 'next/link';
import type { SVGProps } from 'react';

function GitHubMark(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true" {...props}>
      <path d="M12 .7A11.5 11.5 0 0 0 8.36 23.1c.58.1.79-.25.79-.56v-2.23c-3.22.7-3.9-1.37-3.9-1.37-.53-1.34-1.29-1.7-1.29-1.7-1.05-.72.08-.71.08-.71 1.17.08 1.78 1.2 1.78 1.2 1.04 1.77 2.72 1.26 3.38.96.1-.75.4-1.26.74-1.55-2.57-.29-5.27-1.28-5.27-5.68 0-1.26.45-2.28 1.19-3.09-.12-.29-.52-1.46.11-3.04 0 0 .97-.31 3.16 1.18A10.9 10.9 0 0 1 12 6.12c.98 0 1.95.13 2.87.39 2.2-1.49 3.16-1.18 3.16-1.18.63 1.58.23 2.75.11 3.04.74.81 1.19 1.83 1.19 3.09 0 4.41-2.71 5.38-5.29 5.67.42.36.79 1.06.79 2.14v3.27c0 .31.21.67.8.56A11.5 11.5 0 0 0 12 .7Z" />
    </svg>
  );
}

export function HomeNav() {
  return (
    <header className="etl-home-header">
      <nav className="etl-home-nav" aria-label="Main navigation">
        <Link className="etl-home-nav-brand" href="/" aria-label="Supabase ETL home">
          <Brand />
        </Link>

        <div className="etl-home-search">
          <FullSearchTrigger
            hideIfDisabled
            aria-label="Search Supabase ETL documentation"
            className="etl-home-search-full"
          />
          <SearchTrigger
            hideIfDisabled
            aria-label="Search Supabase ETL documentation"
            className="etl-home-search-compact"
          />
        </div>

        <div className="etl-home-nav-links">
          <Link href="/guides/first-pipeline/">Docs</Link>
          <a href={siteConfig.repository} target="_blank" rel="noreferrer">
            <GitHubMark />
            <span>GitHub</span>
          </a>
          <ThemeSwitch className="etl-home-theme-switch" mode="light-dark-system" />
        </div>
      </nav>
    </header>
  );
}

import { Brand, PipelinesMark } from '@/components/brand';
import { siteConfig } from '@/lib/site';
import { source } from '@/lib/source';
import { DocsLayout } from 'fumadocs-ui/layouts/docs';
import type { ReactNode } from 'react';

export default function DocumentationLayout({ children }: { children: ReactNode }) {
  return (
    <DocsLayout
      tree={source.getPageTree()}
      nav={{ title: <Brand />, url: '/', transparentMode: 'none' }}
      githubUrl={siteConfig.repository}
      searchToggle={{
        full: { 'aria-label': 'Search Supabase ETL documentation' },
        sm: { 'aria-label': 'Search Supabase ETL documentation' },
      }}
      sidebar={{
        defaultOpenLevel: 1,
        footer: (
          <a
            className="etl-sidebar-pipelines"
            href={siteConfig.supabaseDocs}
            target="_blank"
            rel="noreferrer"
          >
            <PipelinesMark />
            <span className="etl-sidebar-pipelines-copy">
              <strong>Using Supabase?</strong>
              <small>Try Supabase Pipelines</small>
            </span>
            <span className="etl-sidebar-pipelines-arrow" aria-hidden="true">
              ↗
            </span>
          </a>
        ),
      }}
    >
      {children}
    </DocsLayout>
  );
}

import { getAgentDocSection, getAgentPageGroups, getPageMarkdownUrl, source } from '@/lib/source';
import { absoluteUrl, siteConfig } from '@/lib/site';

export const dynamic = 'force-static';

export function GET() {
  const home = source.getPage([]);
  const orderedPages = [
    ...(home ? [home] : []),
    ...getAgentPageGroups().flatMap(({ pages }) => pages),
  ];
  const pages = orderedPages.map((page) => ({
    title: page.data.title,
    description: page.data.description ?? '',
    section: page.slugs.length === 0 ? 'Overview' : getAgentDocSection(page),
    html_url: absoluteUrl(page.url),
    markdown_url: absoluteUrl(getPageMarkdownUrl(page).url),
    source_url: `${siteConfig.repository}/blob/main/docs/src/content/docs/${page.path}`,
  }));

  return Response.json({
    schema_version: '1.0',
    name: siteConfig.name,
    description: siteConfig.description,
    status: 'under_active_development',
    status_description: siteConfig.projectStatus,
    canonical_url: absoluteUrl('/'),
    repository_url: siteConfig.repository,
    managed_product_url: siteConfig.supabaseDocs,
    discovery: {
      llms_txt: absoluteUrl('/llms.txt'),
      llms_full_txt: absoluteUrl('/llms-full.txt'),
      search_index: absoluteUrl('/api/search'),
    },
    terminology: {
      replication_phases: ['initial sync', 'ongoing replication'],
      primary_verb: 'replicate',
      streaming: 'A transfer mode that may be used within either phase, not a replication phase.',
    },
    pages,
  });
}

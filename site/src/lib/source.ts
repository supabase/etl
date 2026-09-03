import { docs } from 'collections/server';
import { loader } from 'fumadocs-core/source';
import {
  Blocks,
  BookOpenText,
  Database,
  DatabaseZap,
  House,
  ListTree,
  Puzzle,
  RefreshCw,
  Rocket,
  Terminal,
  Workflow,
  type LucideIcon,
} from 'lucide-react';
import { createElement } from 'react';
import { absoluteUrl, siteConfig } from '@/lib/site';

const icons: Record<string, LucideIcon> = {
  Blocks,
  BookOpenText,
  Database,
  DatabaseZap,
  House,
  ListTree,
  Puzzle,
  RefreshCw,
  Rocket,
  Terminal,
  Workflow,
};

export const source = loader({
  baseUrl: '/',
  source: docs.toFumadocsSource(),
  icon(name) {
    const Icon = name ? icons[name] : undefined;
    return Icon ? createElement(Icon, { 'aria-hidden': true, strokeWidth: 1.5 }) : undefined;
  },
});

type SourcePage = (typeof source)['$inferPage'];

export type AgentDocSection = 'Get started' | 'Guides' | 'Concepts' | 'Reference';

/** Returns the public, stable Markdown URL for an agent-readable page. */
export function getPageMarkdownUrl(page: SourcePage) {
  return {
    url: page.slugs.length === 0 ? '/index.md' : `${page.url}.md`,
  };
}

/** Returns the internal static route that generates a page's Markdown source. */
export function getGeneratedPageMarkdownUrl(page: SourcePage) {
  const segments = [...page.slugs, 'content.md'];

  return {
    segments,
    url: `/${['llms.mdx', ...segments].join('/')}`,
  };
}

export function getPageImageUrl(page: (typeof source)['$inferPage']) {
  const segments = [...page.slugs, 'image.png'];

  return {
    segments,
    url: `/${['og', ...segments].join('/')}`,
  };
}

/** Groups documentation by the reader intent used in the sidebar. */
export function getAgentDocSection(page: SourcePage): AgentDocSection {
  if (page.slugs[0] === 'guides') {
    return ['first-pipeline', 'standalone-replicator'].includes(page.slugs[1] ?? '')
      ? 'Get started'
      : 'Guides';
  }

  if (page.slugs[0] === 'reference') return 'Reference';

  return ['events', 'traits'].includes(page.slugs[1] ?? '') ? 'Reference' : 'Concepts';
}

/** Returns agent-readable pages in a deterministic, task-first order. */
export function getAgentPageGroups() {
  const sectionOrder: AgentDocSection[] = [
    'Get started',
    'Guides',
    'Concepts',
    'Reference',
  ];
  const pages = source.getPages().filter((page) => page.slugs.length > 0);
  const pageOrder = new Map([
    ['/guides/first-pipeline', 0],
    ['/guides/standalone-replicator', 1],
    ['/guides/configure-postgres', 2],
    ['/guides/custom-implementations', 3],
    ['/explanation/concepts', 4],
    ['/explanation/architecture', 5],
    ['/explanation/schema-changes', 6],
    ['/reference/destinations', 7],
    ['/explanation/events', 8],
    ['/explanation/traits', 9],
  ]);

  return sectionOrder.map((section) => ({
    section,
    pages: pages
      .filter((page) => getAgentDocSection(page) === section)
      .sort((left, right) => (pageOrder.get(left.url) ?? 99) - (pageOrder.get(right.url) ?? 99)),
  }));
}

/** Returns clean standalone Markdown with canonical and source provenance. */
export async function getLLMText(page: SourcePage) {
  const processed = rewriteAgentLinks(
    rewriteAgentLayout(
      rewriteAgentCallouts(
        page.slugs.length === 0 ? getAgentHomeMarkdown() : await page.data.getText('processed'),
      ),
    ),
  ).trim();
  const sourceUrl = `${siteConfig.repository}/blob/main/site/content/docs/${page.path}`;

  return `# ${page.data.title}\n\n> ${page.data.description ?? ''}\n\n- Canonical HTML: ${absoluteUrl(page.url)}\n- Agent-readable Markdown: ${absoluteUrl(getPageMarkdownUrl(page).url)}\n- Source: ${sourceUrl}\n\n${processed}`;
}

function getAgentHomeMarkdown() {
  const groups = getAgentPageGroups()
    .map(({ section, pages }) => {
      const links = pages
        .map(
          (page) =>
            `- [${page.data.title}](${absoluteUrl(getPageMarkdownUrl(page).url)}): ${page.data.description ?? ''}`,
        )
        .join('\n');

      return `### ${section}\n\n${links}`;
    })
    .join('\n\n');

  return `${siteConfig.description} For each published table, it performs an initial sync of existing rows, then replicates changes to a destination. ${siteConfig.projectStatus}\n\nFor the managed Supabase product, use [Supabase Pipelines](${siteConfig.supabaseDocs}).\n\n## Documentation map\n\n${groups}\n\n## Replication phases\n\n1. **Initial sync:** Copy the existing rows selected by the publication, then catch up changes that occurred while the copy was running.\n2. **Ongoing replication:** Capture subsequent inserts, updates, deletes, and truncates, then deliver those changes as ordered events.\n\nStreaming describes a transfer mode that may be used within either phase; it is not a separate replication phase. Across both phases, a store persists checkpoints, schemas, destination metadata, and table state so replication can recover safely after a restart. Copy and change data capture (CDC) are replication paths, not customer-visible phases.`;
}

function rewriteAgentLinks(markdown: string) {
  return markdown.replace(
    /\]\(\/((?:guides|explanation|reference)\/[^)#]+?)\/?(#[^)]+)?\)/g,
    (_match, path: string, hash: string | undefined) =>
      `](${absoluteUrl(`/${path}.md`)}${hash ?? ''})`,
  );
}

function rewriteAgentCallouts(markdown: string) {
  return markdown.replace(
    /<Callout\b[^>]*\btitle="([^"]+)"[^>]*>\s*([\s\S]*?)\s*<\/Callout>/g,
    (_match, title: string, body: string) => {
      const quotedBody = body
        .trim()
        .split('\n')
        .map((line) => (line.length > 0 ? `> ${line.trimStart()}` : '>'))
        .join('\n');

      return `> **${title}**\n>\n${quotedBody}`;
    },
  );
}

function rewriteAgentLayout(markdown: string) {
  return markdown
    .replace(
      /<DestinationStatus status="(stable|in-progress|deprecated)"\s*\/>/g,
      (_match, status: string) =>
        `**Status: ${{ stable: 'Stable', 'in-progress': 'In progress', deprecated: 'Deprecated' }[status]}**`,
    )
    .replace(/^\s*<div className="fd-(?:steps|step)">\s*$/gm, '')
    .replace(/^\s*<\/div>\s*$/gm, '')
    .replace(/^ {4}/gm, '');
}

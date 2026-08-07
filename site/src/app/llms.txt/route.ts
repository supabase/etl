import { getAgentPageGroups, getPageMarkdownUrl } from '@/lib/source';
import { absoluteUrl, siteConfig } from '@/lib/site';

export const revalidate = false;

export function GET() {
  const sections = getAgentPageGroups()
    .map(({ section, pages }) => {
      const links = pages
        .map(
          (page) =>
            `- [${page.data.title}](${absoluteUrl(getPageMarkdownUrl(page).url)}): ${page.data.description ?? ''}`,
        )
        .join('\n');

      return `## ${section}\n\n${links}`;
    })
    .join('\n\n');

  const text = `# ${siteConfig.name}\n\n> ${siteConfig.description}\n\n${siteConfig.projectStatus} These pages document the open-source replication engine; managed-product guidance belongs in Supabase Pipelines.\n\n${sections}\n\n## Complete context\n\n- [Full documentation in one file](${absoluteUrl('/llms-full.txt')})\n- [Agent manifest](${absoluteUrl('/agents.json')})\n\n## Project\n\n- [Canonical documentation](${absoluteUrl('/')})\n- [GitHub repository](${siteConfig.repository})\n- [Managed Supabase Pipelines](${siteConfig.supabaseDocs})`;

  return new Response(text, {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
}

import { getLLMText, source } from '@/lib/source';
import { absoluteUrl } from '@/lib/site';

export const revalidate = false;

export async function GET() {
  const pages = await Promise.all(source.getPages().map(getLLMText));
  const preamble = `# Supabase ETL Documentation\n\n> Complete agent-readable context for the open-source Supabase ETL Rust framework.\n\n- Status: Under active development\n- Canonical documentation: ${absoluteUrl('/')}\n- Repository: https://github.com/supabase/etl\n- Managed product: https://supabase.com/docs/guides/database/replication/pipelines\n\nTerminology: the two replication phases are **initial sync** and **ongoing replication**. Supabase ETL initially syncs existing table rows, then replicates subsequent changes and delivers them to the destination. Streaming is a transfer mode, not a replication phase.`;

  return new Response([preamble, ...pages].join('\n\n---\n\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
}

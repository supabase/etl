export const siteConfig = {
  name: 'Supabase ETL',
  shortName: 'ETL',
  title: 'Supabase ETL — Postgres change data capture for Rust',
  description:
    'Perform an initial sync of Postgres tables, then replicate changes to destinations with Supabase ETL.',
  origin: 'https://supabase.github.io',
  basePath: '/etl',
  repository: 'https://github.com/supabase/etl',
  supabaseDocs: 'https://supabase.com/docs/guides/database/replication/pipelines',
} as const;

/** Adds the GitHub Pages base path to an application-relative path. */
export function withBasePath(path: string): string {
  if (/^https?:\/\//.test(path) || path === siteConfig.basePath || path.startsWith(`${siteConfig.basePath}/`)) {
    return path;
  }

  const normalized = path.startsWith('/') ? path : `/${path}`;
  return normalized === '/' ? `${siteConfig.basePath}/` : `${siteConfig.basePath}${normalized}`;
}

/** Returns the public canonical URL for an application-relative path. */
export function absoluteUrl(path: string): string {
  return new URL(withBasePath(path), siteConfig.origin).toString();
}

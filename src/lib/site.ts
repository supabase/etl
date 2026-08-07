export const siteConfig = {
  name: 'Supabase ETL',
  shortName: 'ETL',
  title: 'Supabase ETL — High-performance Postgres replication in Rust',
  description:
    'A high-performance Postgres replication engine written in Rust. Embed it in your Rust application or run it as a standalone binary.',
  projectStatus:
    'Supabase ETL is under active development. APIs and setup steps may change before the first stable release.',
  // Overridable so a fork, preview deployment, or custom domain does not bake in
  // supabase.github.io/etl. Keep the defaults in sync with `next.config.mjs`.
  origin: process.env.SITE_ORIGIN ?? 'https://supabase.github.io',
  basePath: process.env.SITE_BASE_PATH ?? '/etl',
  repository: 'https://github.com/supabase/etl',
  supabaseDocs: 'https://supabase.com/docs/guides/database/replication/pipelines',
} as const;

function isStaticFileOrApiPath(pathname: string): boolean {
  const applicationPath = pathname.startsWith(siteConfig.basePath)
    ? pathname.slice(siteConfig.basePath.length) || '/'
    : pathname;

  return applicationPath.startsWith('/api/') || /\/[^/]+\.[^/]+$/.test(applicationPath);
}

function withCanonicalTrailingSlash(pathname: string): string {
  if (pathname.endsWith('/') || isStaticFileOrApiPath(pathname)) return pathname;

  return `${pathname}/`;
}

/** Adds the GitHub Pages base path and canonical page trailing slash. */
export function withBasePath(path: string): string {
  if (/^https?:\/\//.test(path)) return path;

  const [, pathname = '', suffix = ''] = path.match(/^([^?#]*)([?#].*)?$/) ?? [];
  const normalized = pathname.startsWith('/') ? pathname : `/${pathname}`;
  const withBase =
    normalized === '/' || normalized === siteConfig.basePath
      ? `${siteConfig.basePath}/`
      : normalized.startsWith(`${siteConfig.basePath}/`)
        ? normalized
        : `${siteConfig.basePath}${normalized}`;

  return `${withCanonicalTrailingSlash(withBase)}${suffix}`;
}

/** Returns the public canonical URL for an application-relative path. */
export function absoluteUrl(path: string): string {
  return new URL(withBasePath(path), siteConfig.origin).toString();
}

import { createMDX } from 'fumadocs-mdx/next';

const withMDX = createMDX();

// Overridable so this export can be hosted somewhere other than
// supabase.github.io/etl (a fork, a preview deployment, a custom domain).
// Keep in sync with the default in `src/lib/site.ts`.
const basePath = process.env.SITE_BASE_PATH ?? '/etl';

/** @type {import('next').NextConfig} */
const config = {
  output: 'export',
  basePath,
  assetPrefix: basePath,
  trailingSlash: true,
  reactStrictMode: true,
  images: {
    unoptimized: true,
  },
};

export default withMDX(config);
